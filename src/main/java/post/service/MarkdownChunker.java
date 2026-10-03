package post.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.commonmark.ext.gfm.tables.TableBlock;
import org.commonmark.node.BlockQuote;
import org.commonmark.node.BulletList;
import org.commonmark.node.FencedCodeBlock;
import org.commonmark.node.Heading;
import org.commonmark.node.HtmlBlock;
import org.commonmark.node.IndentedCodeBlock;
import org.commonmark.node.Node;
import org.commonmark.node.OrderedList;
import org.commonmark.node.Paragraph;
import org.commonmark.node.SourceSpan;
import org.commonmark.node.ThematicBreak;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.text.TextContentRenderer;
import org.springframework.ai.tokenizer.TokenCountEstimator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * Splits Markdown at AST block boundaries while retaining the exact source text.
 */
@Component
public class MarkdownChunker {

    static final int TARGET_TOKENS = 300;
    static final int MIN_TOKENS = 100;
    static final int MAX_TOKENS = 600;

    private final Parser markdownParser;
    private final TextContentRenderer markdownTextRenderer;
    private final TokenCountEstimator tokenCountEstimator;

    public MarkdownChunker(
            @Qualifier("markdownParser") Parser markdownParser,
            @Qualifier("markdownTextRenderer") TextContentRenderer markdownTextRenderer,
            @Qualifier("markdownTokenCountEstimator") TokenCountEstimator tokenCountEstimator
    ) {
        this.markdownParser = markdownParser;
        this.markdownTextRenderer = markdownTextRenderer;
        this.tokenCountEstimator = tokenCountEstimator;
    }

    public List<MarkdownChunk> chunk(String originalMarkdown) {
        Objects.requireNonNull(originalMarkdown, "originalMarkdown 不能为空");
        if (originalMarkdown.isEmpty()) {
            return List.of();
        }
        return assembleChunks(mapBlocks(originalMarkdown));
    }

    /**
     * Maps only top-level AST nodes. Nested list and quote children stay inside their
     * owning block, so source text is never duplicated.
     */
    List<MarkdownBlock> mapBlocks(String originalMarkdown) {
        Node document = markdownParser.parse(originalMarkdown);
        List<Node> astBlocks = topLevelBlocks(document);
        if (astBlocks.isEmpty()) {
            return List.of(new MarkdownBlock(
                    BlockType.OTHER,
                    originalMarkdown,
                    List.of(),
                    estimate(originalMarkdown),
                    0,
                    originalMarkdown.length(),
                    0,
                    false
            ));
        }

        List<Integer> sourceStarts = new ArrayList<>(astBlocks.size());
        for (Node astBlock : astBlocks) {
            int sourceStart = firstSourceOffset(astBlock);
            if (sourceStart < 0) {
                throw new IllegalStateException(
                        "Markdown AST 块缺少 source span: " + astBlock.getClass().getSimpleName()
                );
            }
            sourceStarts.add(sourceStart);
        }

        List<MarkdownBlock> blocks = new ArrayList<>(astBlocks.size());
        HeadingSegment[] headingLevels = new HeadingSegment[6];
        for (int i = 0; i < astBlocks.size(); i++) {
            Node astBlock = astBlocks.get(i);
            int startOffset = i == 0 ? 0 : sourceStarts.get(i);
            int endOffset = i + 1 < astBlocks.size()
                    ? sourceStarts.get(i + 1)
                    : originalMarkdown.length();
            if (startOffset < 0 || endOffset < startOffset || endOffset > originalMarkdown.length()) {
                throw new IllegalStateException("Markdown AST source span 顺序无效");
            }

            int headingLevel = 0;
            if (astBlock instanceof Heading heading) {
                headingLevel = heading.getLevel();
                Arrays.fill(headingLevels, headingLevel - 1, headingLevels.length, null);
                String headingText = markdownTextRenderer.render(heading).strip();
                headingLevels[headingLevel - 1] = new HeadingSegment(headingLevel, headingText);
            }

            BlockType type = blockType(astBlock);
            String rawMarkdown = originalMarkdown.substring(startOffset, endOffset);
            blocks.add(new MarkdownBlock(
                    type,
                    rawMarkdown,
                    currentHeadingPath(headingLevels),
                    estimate(rawMarkdown),
                    startOffset,
                    endOffset,
                    headingLevel,
                    isStrongBoundary(type, headingLevel)
            ));
        }
        return List.copyOf(blocks);
    }

    List<MarkdownChunk> assembleChunks(List<MarkdownBlock> blocks) {
        if (blocks.isEmpty()) {
            return List.of();
        }

        List<MarkdownChunk> chunks = new ArrayList<>();
        ChunkAccumulator currentChunk = new ChunkAccumulator();

        for (int index = 0; index < blocks.size();) {
            List<MarkdownBlock> groupedBlocks = new ArrayList<>(2);
            MarkdownBlock currentNode = blocks.get(index);
            groupedBlocks.add(currentNode);
            if (shouldPairWithNext(currentNode, blocks, index)) {
                groupedBlocks.add(blocks.get(index + 1));
                index += 2;
            } else {
                index++;
            }

            CandidateGroup candidateGroup = new CandidateGroup(groupedBlocks);
            for (CandidateGroup safeCandidate : enforceHardLimit(candidateGroup)) {
                // All priority checks use the chunk as it existed at loop entry.
                ChunkAccumulator currentSnapshot = currentChunk.copy();

                // Priority 0: an empty chunk accepts the candidate immediately.
                if (currentSnapshot.isEmpty()) {
                    currentSnapshot.add(safeCandidate);
                    currentChunk = currentSnapshot;
                    continue;
                }

                // Priority 1: H1/thematic-break boundaries always close the old chunk.
                // Document and permission boundaries are invocation boundaries: one
                // saveVectorStore call contains one document in one permission scope.
                if (safeCandidate.startsAtStrongBoundary()) {
                    chunks.add(currentSnapshot.toChunk());
                    currentChunk = startChunk(safeCandidate);
                    continue;
                }
                List<HeadingSegment> lastHeadingPath = currentSnapshot.lastHeadingPath();
                List<HeadingSegment> candidateHeadingPath = safeCandidate.firstHeadingPath();

                // Priority 2: blocks on the same heading path merge up to the hard limit.
                if (lastHeadingPath.equals(candidateHeadingPath)) {
                    int mergedTokenCount = estimate(currentSnapshot.text() + safeCandidate.text());
                    if (mergedTokenCount <= MAX_TOKENS) {
                        currentSnapshot.add(safeCandidate);
                        currentChunk = currentSnapshot;
                    } else {
                        chunks.add(currentSnapshot.toChunk());
                        currentChunk = startChunk(safeCandidate);
                    }
                    continue;
                }

                // Priority 3: a heading-path change closes a chunk that has reached
                // the hard minimum.
                if (currentSnapshot.tokenCount() >= MIN_TOKENS) {
                    chunks.add(currentSnapshot.toChunk());
                    currentChunk = startChunk(safeCandidate);
                    continue;
                }

                // Priority 4: a sub-minimum chunk crosses a heading-path boundary
                // only when the two paths represent siblings or parent/child nodes.
                if (areSiblingOrParentChild(lastHeadingPath, candidateHeadingPath)) {
                    currentSnapshot.add(safeCandidate);
                    currentChunk = currentSnapshot;
                } else {
                    chunks.add(currentSnapshot.toChunk());
                    currentChunk = startChunk(safeCandidate);
                }
            }
        }

        if (!currentChunk.isEmpty()) {
            chunks.add(currentChunk.toChunk());
        }
        return List.copyOf(chunks);
    }

    private ChunkAccumulator startChunk(CandidateGroup candidate) {
        ChunkAccumulator accumulator = new ChunkAccumulator();
        accumulator.add(candidate);
        return accumulator;
    }

    private boolean shouldPairWithNext(
            MarkdownBlock currentNode,
            List<MarkdownBlock> blocks,
            int currentIndex
    ) {
        if (currentNode.type() != BlockType.PARAGRAPH || currentIndex + 1 >= blocks.size()) {
            return false;
        }
        BlockType nextType = blocks.get(currentIndex + 1).type();
        return nextType == BlockType.LIST
                || nextType == BlockType.CODE_BLOCK
                || nextType == BlockType.BLOCK_QUOTE;
    }

    private List<CandidateGroup> enforceHardLimit(CandidateGroup candidate) {
        if (estimate(candidate.text()) <= MAX_TOKENS) {
            return List.of(candidate);
        }

        List<CandidateGroup> safeCandidates = new ArrayList<>();
        for (MarkdownBlock block : candidate.blocks()) {
            for (MarkdownBlock safeBlock : splitOversizedBlock(block)) {
                safeCandidates.add(new CandidateGroup(List.of(safeBlock)));
            }
        }
        return safeCandidates;
    }

    private List<MarkdownBlock> splitOversizedBlock(MarkdownBlock block) {
        if (block.tokenCount() <= MAX_TOKENS) {
            return List.of(block);
        }

        List<MarkdownBlock> fragments = new ArrayList<>();
        String rawMarkdown = block.rawMarkdown();
        int localStart = 0;
        boolean firstFragment = true;
        while (localStart < rawMarkdown.length()) {
            int hardEnd = largestFittingEnd(rawMarkdown, localStart, MAX_TOKENS);
            int localEnd;
            if (hardEnd == rawMarkdown.length()) {
                localEnd = rawMarkdown.length();
            } else {
                int targetEnd = Math.min(
                        largestFittingEnd(rawMarkdown, localStart, TARGET_TOKENS),
                        hardEnd
                );
                localEnd = preferredNaturalEnd(
                        rawMarkdown,
                        block.type(),
                        localStart,
                        targetEnd,
                        hardEnd
                );
                if (localEnd <= localStart) {
                    localEnd = targetEnd;
                }
                if (estimate(rawMarkdown.substring(localStart, localEnd)) > MAX_TOKENS) {
                    // A tokenizer is not required to be strictly monotonic for every
                    // prefix. targetEnd was measured directly and is the safe fallback.
                    localEnd = targetEnd;
                }
            }

            String fragmentText = rawMarkdown.substring(localStart, localEnd);
            int fragmentTokens = estimate(fragmentText);
            if (fragmentTokens > MAX_TOKENS) {
                throw new IllegalStateException("无法在 Markdown 硬上限内安全切分块");
            }
            fragments.add(new MarkdownBlock(
                    block.type(),
                    fragmentText,
                    block.headingPath(),
                    fragmentTokens,
                    block.startOffset() + localStart,
                    block.startOffset() + localEnd,
                    block.headingLevel(),
                    firstFragment && block.strongBoundary()
            ));
            firstFragment = false;
            localStart = localEnd;
        }
        return fragments;
    }

    private int largestFittingEnd(String text, int start, int tokenLimit) {
        int best = start;
        int maximumDistance = text.length() - start;
        int probeDistance = Math.min(256, maximumDistance);
        int failedEnd;

        while (true) {
            int probe = safeEndAtOrBefore(text, start, start + probeDistance);
            if (probe <= start) {
                probe = nextSafeEnd(text, start);
            }
            if (estimate(text.substring(start, probe)) > tokenLimit) {
                failedEnd = probe;
                break;
            }

            best = probe;
            if (probe == text.length()) {
                return best;
            }

            int consumed = probe - start;
            long doubledDistance = Math.max(consumed + 1L, consumed * 2L);
            probeDistance = (int) Math.min(maximumDistance, doubledDistance);
        }

        int low = best + 1;
        int high = failedEnd - 1;
        while (low <= high) {
            int middle = low + (high - low) / 2;
            int safeMiddle = safeEndAtOrBefore(text, start, middle);
            if (safeMiddle <= start) {
                safeMiddle = nextSafeEnd(text, start);
            }
            int tokens = estimate(text.substring(start, safeMiddle));
            if (tokens <= tokenLimit) {
                best = Math.max(best, safeMiddle);
                low = middle + 1;
            } else {
                high = middle - 1;
            }
        }

        if (best == start) {
            best = nextSafeEnd(text, start);
            if (estimate(text.substring(start, best)) > tokenLimit) {
                throw new IllegalStateException("单个 Unicode 字符超过 Markdown token 硬上限");
            }
        }

        // Finish with a bounded code-point scan up to the first measured failure.
        // This keeps the returned prefix safe without tokenizing the whole remainder.
        while (best < failedEnd) {
            int next = nextSafeEnd(text, best);
            if (next > failedEnd || estimate(text.substring(start, next)) > tokenLimit) {
                break;
            }
            best = next;
        }
        return best;
    }

    private int preferredNaturalEnd(
            String text,
            BlockType type,
            int start,
            int targetEnd,
            int hardEnd
    ) {
        int nextNaturalEnd = -1;
        for (int end = targetEnd; end <= hardEnd; end++) {
            if (isNaturalBoundary(text, type, end)) {
                nextNaturalEnd = end;
                break;
            }
        }

        int minimumPreferredEnd = start + Math.max(1, (targetEnd - start) / 2);
        int previousNaturalEnd = -1;
        for (int end = targetEnd; end >= minimumPreferredEnd; end--) {
            if (isNaturalBoundary(text, type, end)) {
                previousNaturalEnd = end;
                break;
            }
        }
        if (nextNaturalEnd < 0) {
            return previousNaturalEnd < 0 ? targetEnd : previousNaturalEnd;
        }
        if (previousNaturalEnd < 0) {
            return nextNaturalEnd;
        }
        if (nextNaturalEnd - targetEnd <= targetEnd - previousNaturalEnd) {
            return nextNaturalEnd;
        }
        return previousNaturalEnd;
    }

    private static boolean isNaturalBoundary(String text, BlockType type, int end) {
        if (end <= 0 || end > text.length()) {
            return false;
        }
        char character = text.charAt(end - 1);
        if (character == '\n') {
            return true;
        }
        if (character == '\r' && end < text.length() && text.charAt(end) == '\n') {
            return false;
        }

        if (type == BlockType.PARAGRAPH
                || type == BlockType.HEADING
                || type == BlockType.HTML
                || type == BlockType.OTHER) {
            return character == '。'
                    || character == '！'
                    || character == '？'
                    || character == '.'
                    || character == '!'
                    || character == '?'
                    || Character.isWhitespace(character);
        }
        return false;
    }

    private static int safeEndAtOrBefore(String text, int start, int end) {
        int safeEnd = Math.min(end, text.length());
        if (safeEnd > start
                && safeEnd < text.length()
                && Character.isHighSurrogate(text.charAt(safeEnd - 1))
                && Character.isLowSurrogate(text.charAt(safeEnd))) {
            safeEnd--;
        }
        if (safeEnd > start
                && safeEnd < text.length()
                && text.charAt(safeEnd - 1) == '\r'
                && text.charAt(safeEnd) == '\n') {
            safeEnd--;
        }
        return safeEnd;
    }

    private static int nextSafeEnd(String text, int start) {
        if (start >= text.length()) {
            return text.length();
        }
        char first = text.charAt(start);
        if (first == '\r' && start + 1 < text.length() && text.charAt(start + 1) == '\n') {
            return start + 2;
        }
        if (Character.isHighSurrogate(first)
                && start + 1 < text.length()
                && Character.isLowSurrogate(text.charAt(start + 1))) {
            return start + 2;
        }
        return start + 1;
    }

    private int estimate(String text) {
        return tokenCountEstimator.estimate(text);
    }

    private static List<Node> topLevelBlocks(Node document) {
        List<Node> blocks = new ArrayList<>();
        for (Node child = document.getFirstChild(); child != null; child = child.getNext()) {
            blocks.add(child);
        }
        return blocks;
    }

    private static int firstSourceOffset(Node node) {
        int firstOffset = Integer.MAX_VALUE;
        for (SourceSpan sourceSpan : node.getSourceSpans()) {
            firstOffset = Math.min(firstOffset, sourceSpan.getInputIndex());
        }
        for (Node child = node.getFirstChild(); child != null; child = child.getNext()) {
            int childOffset = firstSourceOffset(child);
            if (childOffset >= 0) {
                firstOffset = Math.min(firstOffset, childOffset);
            }
        }
        return firstOffset == Integer.MAX_VALUE ? -1 : firstOffset;
    }

    private static BlockType blockType(Node node) {
        if (node instanceof Heading) {
            return BlockType.HEADING;
        }
        if (node instanceof Paragraph) {
            return BlockType.PARAGRAPH;
        }
        if (node instanceof BulletList || node instanceof OrderedList) {
            return BlockType.LIST;
        }
        if (node instanceof BlockQuote) {
            return BlockType.BLOCK_QUOTE;
        }
        if (node instanceof FencedCodeBlock || node instanceof IndentedCodeBlock) {
            return BlockType.CODE_BLOCK;
        }
        if (node instanceof TableBlock) {
            return BlockType.TABLE;
        }
        if (node instanceof ThematicBreak) {
            return BlockType.THEMATIC_BREAK;
        }
        if (node instanceof HtmlBlock) {
            return BlockType.HTML;
        }
        return BlockType.OTHER;
    }

    private static boolean isStrongBoundary(BlockType type, int headingLevel) {
        return (type == BlockType.HEADING && headingLevel == 1)
                || type == BlockType.THEMATIC_BREAK;
    }

    private static List<HeadingSegment> currentHeadingPath(HeadingSegment[] headingLevels) {
        List<HeadingSegment> path = new ArrayList<>(headingLevels.length);
        for (HeadingSegment heading : headingLevels) {
            if (heading != null) {
                path.add(heading);
            }
        }
        return List.copyOf(path);
    }

    private static boolean areSiblingOrParentChild(
            List<HeadingSegment> firstPath,
            List<HeadingSegment> secondPath
    ) {
        if (isStrictPrefix(firstPath, secondPath) || isStrictPrefix(secondPath, firstPath)) {
            return true;
        }
        if (firstPath.isEmpty() || firstPath.size() != secondPath.size()) {
            return false;
        }
        return firstPath.subList(0, firstPath.size() - 1)
                .equals(secondPath.subList(0, secondPath.size() - 1));
    }

    private static boolean isStrictPrefix(
            List<HeadingSegment> possibleParent,
            List<HeadingSegment> possibleChild
    ) {
        return possibleParent.size() < possibleChild.size()
                && possibleParent.equals(possibleChild.subList(0, possibleParent.size()));
    }

    enum BlockType {
        HEADING,
        PARAGRAPH,
        LIST,
        BLOCK_QUOTE,
        CODE_BLOCK,
        TABLE,
        THEMATIC_BREAK,
        HTML,
        OTHER
    }

    record HeadingSegment(int level, String title) {
        HeadingSegment {
            Objects.requireNonNull(title, "title 不能为空");
        }
    }

    record MarkdownBlock(
            BlockType type,
            String rawMarkdown,
            List<HeadingSegment> headingPath,
            int tokenCount,
            int startOffset,
            int endOffset,
            int headingLevel,
            boolean strongBoundary
    ) {
        MarkdownBlock {
            Objects.requireNonNull(type, "type 不能为空");
            Objects.requireNonNull(rawMarkdown, "rawMarkdown 不能为空");
            headingPath = List.copyOf(headingPath);
        }
    }

    public record MarkdownChunk(
            String text,
            List<String> headingPaths,
            List<String> blockTypes,
            int tokenCount,
            int startOffset,
            int endOffset
    ) {
        public MarkdownChunk {
            Objects.requireNonNull(text, "text 不能为空");
            headingPaths = List.copyOf(headingPaths);
            blockTypes = List.copyOf(blockTypes);
        }
    }

    private record CandidateGroup(List<MarkdownBlock> blocks) {
        private CandidateGroup {
            if (blocks.isEmpty()) {
                throw new IllegalArgumentException("candidateGroup 不能为空");
            }
            blocks = List.copyOf(blocks);
        }

        private String text() {
            return blocks.stream()
                    .map(MarkdownBlock::rawMarkdown)
                    .collect(Collectors.joining());
        }

        private boolean startsAtStrongBoundary() {
            return blocks.get(0).strongBoundary();
        }

        private List<HeadingSegment> firstHeadingPath() {
            return blocks.get(0).headingPath();
        }
    }

    private final class ChunkAccumulator {
        private final List<MarkdownBlock> blocks;
        private final StringBuilder text;
        private int tokenCount;

        private ChunkAccumulator() {
            this.blocks = new ArrayList<>();
            this.text = new StringBuilder();
        }

        private ChunkAccumulator(ChunkAccumulator source) {
            this.blocks = new ArrayList<>(source.blocks);
            this.text = new StringBuilder(source.text);
            this.tokenCount = source.tokenCount;
        }

        private ChunkAccumulator copy() {
            return new ChunkAccumulator(this);
        }

        private boolean isEmpty() {
            return blocks.isEmpty();
        }

        private void add(CandidateGroup candidate) {
            if (!blocks.isEmpty()) {
                int expectedStart = blocks.get(blocks.size() - 1).endOffset();
                int actualStart = candidate.blocks().get(0).startOffset();
                if (expectedStart != actualStart) {
                    throw new IllegalStateException("Markdown 块的 source span 不连续");
                }
            }
            blocks.addAll(candidate.blocks());
            text.append(candidate.text());
            tokenCount = estimate(text.toString());
        }

        private String text() {
            return text.toString();
        }

        private int tokenCount() {
            return tokenCount;
        }

        private List<HeadingSegment> lastHeadingPath() {
            return blocks.get(blocks.size() - 1).headingPath();
        }

        private MarkdownChunk toChunk() {
            if (isEmpty()) {
                throw new IllegalStateException("空 Chunk 不能输出");
            }
            Set<String> headingPaths = new LinkedHashSet<>();
            Set<String> blockTypes = new LinkedHashSet<>();
            for (MarkdownBlock block : blocks) {
                if (!block.headingPath().isEmpty()) {
                    headingPaths.add(formatHeadingPath(block.headingPath()));
                }
                blockTypes.add(block.type().name());
            }
            return new MarkdownChunk(
                    text(),
                    List.copyOf(headingPaths),
                    List.copyOf(blockTypes),
                    tokenCount,
                    blocks.get(0).startOffset(),
                    blocks.get(blocks.size() - 1).endOffset()
            );
        }
    }

    private static String formatHeadingPath(List<HeadingSegment> headingPath) {
        return headingPath.stream()
                .map(HeadingSegment::title)
                .collect(Collectors.joining(" > "));
    }
}
