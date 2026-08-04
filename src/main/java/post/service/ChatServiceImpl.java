package post.service;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.ToolResponseMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.document.Document;
import org.springframework.ai.model.tool.ToolCallingChatOptions;
import org.springframework.ai.model.tool.ToolCallingManager;
import org.springframework.ai.model.tool.ToolExecutionResult;
import org.springframework.ai.support.ToolCallbacks;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.ai.vectorstore.filter.FilterExpressionBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

import post.mapper.PostMapper;
import post.memory.SummarizingWindowChatMemory;
import post.model.Message;
import post.model.Post;
import post.tool.ArticleSearchTool;
import post.tool.AuthorArticlesTool;

@Slf4j
@Service
public class ChatServiceImpl implements ChatService {

    private static final double LONG_MEMORY_SIMILARITY_THRESHOLD = 0.70D;
    private static final int MAX_REACT_ITERATIONS = 3;
    private static final int MAX_REACT_TOOL_CALLS = 2;
    private static final int MAX_CONSECUTIVE_NO_NEW_EVIDENCE = 2;

    private static final String STANDALONE_QUESTION_SYSTEM_PROMPT = """
            你是问题改写助手。请结合文章标题和对话历史，将用户的当前问题改写为一个
            无需依赖对话上下文即可理解的独立问题。
            保留原问题的意图、条件和指代对象，不要回答问题，不要补充未知事实。
            只输出改写后的问题，不要输出解释、前缀或引号。
            """;

    private static final String MEMORY_RETRIEVAL_PLANNER_SYSTEM_PROMPT = """
            你是知识社区 Agent 的长期记忆检索规划器。

            你的任务不是回答用户问题，而是根据消除指代后的独立问题和当前有效上下文，生成本轮长期记忆检索计划，并判断后续是否需要检索文章知识库。

            你需要同时完成：

            1. 判断是否精确查询 tech_stack；
            2. 判断是否精确查询 skill_level；
            3. 判断是否向量检索 topics；
            4. 判断是否向量检索 current_goal；
            5. 判断是否需要继续检索文章知识库。

            ====================
            一、精确长期记忆
            ====================

            1. tech_stack

            含义：
            用户当前项目使用的编程语言、框架、数据库、中间件和基础设施。

            以下情况可以查询：

            - 用户明确要求结合“我的项目”或“我的技术栈”；
            - 回答依赖用户当前项目环境；
            - 涉及技术选型、架构设计、方案适配或具体实现；
            - 需要判断某个方案是否兼容用户现有技术。

            以下情况通常不查询：

            - 单纯解释一个通用技术概念；
            - 回答不依赖用户项目环境。

            2. skill_level

            含义：
            用户对某项具体技术的掌握程度。

            以下情况可以查询：

            - 用户要求按照自己的水平讲解；
            - 需要制定学习路线；
            - 需要安排练习、任务或学习难度；
            - 回答深度明显依赖用户对具体技术的掌握程度。

            ====================
            二、向量长期记忆
            ====================

            1. topics

            含义：
            用户长期关注、学习或持续研究的主题。

            只有以下情况才检索：

            - 用户要求结合自己的兴趣或长期研究方向；
            - 用户要求推荐文章主题、学习方向或研究内容；
            - 用户当前问题与长期关注方向之间的关联会明显改善回答。

            topics.query 必须描述：

            与当前问题相关的用户长期关注、学习或研究方向。

            不能只写：

            - 用户兴趣
            - 用户主题
            - 用户关注什么

            2. current_goal

            含义：
            用户正在持续推进的项目、任务或中长期目标。

            以下情况可以检索：

            - 用户询问下一步做什么；
            - 用户要求结合自己的项目目标；
            - 用户询问优先级、MVP 范围或实施路线；
            - 判断某个方案是否适合用户正在推进的项目。

            current_goal.query 必须描述：

            与当前问题相关的用户正在推进的项目、任务或目标。

            不能只写：

            - 用户目标
            - 用户当前计划
            - 用户要做什么

            topics 和 current_goal 的 top_k 固定为 2。

            ====================
            三、文章知识库判断
            ====================

            article_search_needed 表示后续是否需要生成文章知识库检索查询。

            以下情况通常为 true：

            - 用户要求查找、推荐、总结、比较或解释社区文章；
            - 用户针对当前文章或选中文本提问；
            - 回答需要从社区文章中获取技术证据；
            - 用户的技术问题需要通过文章知识库 RAG 回答；
            - 用户要求寻找实现方案、教程、案例或相关资料。

            以下情况通常为 false：

            - 问候或闲聊；
            - 不需要知识证据的简单操作；
            - 当前上下文已经提供了回答所需的完整信息；
            - 用户明确要求不检索文章知识库。

            此步骤只判断是否需要文章检索，不生成文章检索 query。
            文章检索 query 将在长期记忆查询完成后，由另一个模型生成。

            ====================
            四、通用规则
            ====================

            1. 只查询回答当前问题确实需要的长期记忆。
            2. 不要为了增加个性化而查询无关记忆。
            3. 普通知识问答通常不查询用户长期记忆。
            4. 不得创造新的 memory_key。
            6. 每种向量记忆只能生成一个 query。
            7. 向量 query 必须能够脱离对话独立理解。
            8. 向量 query 必须包含当前讨论的领域、对象或任务。
            9. 不得编造用户没有表达过的技术栈、兴趣或目标。
            10. 不生成 SQL。
            11. 不生成文章检索 query。
            12. 不生成最终回答。
            13. 不输出原因、Markdown、代码块或额外说明。
            14. 始终返回全部规定字段。
            15. 只输出合法 JSON。

            固定输出格式：

            {
              "exact_memory": {
                "tech_stack": false,
                "skill_level": {
                  "needed": false
                }
              },
              "vector_memory": {
                "topics": {
                  "needed": false,
                  "query": null,
                  "top_k": 2
                },
                "current_goal": {
                  "needed": false,
                  "query": null,
                  "top_k": 2
                }
              },
              "article_search_needed": false
            }

            字段约束：

            1. topics.needed=false 时，topics.query 必须为 null。
            2. topics.needed=true 时，topics.query 不能为空。
            3. current_goal.needed=false 时，current_goal.query 必须为 null。
            4. current_goal.needed=true 时，current_goal.query 不能为空。
            5. top_k 始终为 2。
            """;

    @SuppressWarnings("unused")
    private static final String ARTICLE_SEARCH_QUERY_GENERATOR_SYSTEM_PROMPT = """
            你是知识社区文章检索查询生成器。

            请根据用户的独立问题、页面上下文和相关长期记忆，生成文章知识库检索计划。

            输出内容包括：

            1. needed
               是否需要检索文章知识库。

            2. semantic_query
               用于向量检索。
               必须是完整、自然、可以脱离当前对话独立理解的信息需求。

            3. keywords
               用于 BM25 或倒排索引。
               只保留技术名、类名、方法名、错误码、版本号、业务实体和精确术语。

            规则：

            1. 只使用与当前问题有关的长期记忆。
            2. 不要把 language 和 answer_style 放入检索 query。
            3. 不得编造用户技术栈。
            4. 不得输出权限、审核、删除和发布状态条件。
            5. 权限由后端处理。
            6. 最多输出 8 个关键词。
            7. 不生成答案。
            8. 只输出合法 JSON。

            输出格式：

            {
              "needed": true,
              "semantic_query": "文章向量检索 query",
              "keywords": [
                "关键词"
              ]
             }
            """;

    @SuppressWarnings("unused")
    private static final String REACT_DECISION_CONTROLLER_SYSTEM_PROMPT = """
            你是一个 ReAct 智能体中的“思考与决策控制器”。

            你的任务是读取完整的 AgentState，分析当前问题、对话记忆、长期记忆、已有证据和执行限制，然后决定下一步应该：

            1. 直接生成最终答案；
            2. 调用一次工具补充证据；
            3. 请求用户提供必要信息；
            4. 因达到限制或发生错误而终止。

            你只负责做出决策，不负责真正执行工具，也不直接输出面向用户的最终答案。

            ## 一、输入数据

            你会收到一个完整的 AgentState JSON，可能包含以下字段：

            ```json
            {
              "original_question": "用户原始问题",
              "independent_question": "经过上下文消解后的独立问题",
              "conversation_memory": [
                {
                  "role": "user 或 assistant",
                  "content": "历史对话内容"
                }
              ],
              "long_term_memory": {
                "exact": [
                  {
                    "memory_key": "记忆键",
                    "content": "明确的长期记忆"
                  }
                ],
                "semantic": [
                  {
                    "memory_key": "记忆键",
                    "content": "语义相关记忆",
                    "score": 0.0
                  }
                ]
              },
              "react_control": {
                "status": "RUNNING",
                "iteration": 0,
                "max_iterations": 3,
                "tool_call_count": 0,
                "max_tool_calls": 2,
                "used_queries": [],
                "visited_chunk_ids": [],
                "consecutive_no_new_evidence": 0,
                "stop_reason": null
              },
              "evidences": []
            }
            ```

            ## 二、字段使用优先级

            分析问题时，按照以下优先级使用信息：

            1. `independent_question`
            2. `original_question`
            3. `conversation_memory`
            4. `long_term_memory.exact`
            5. `long_term_memory.semantic`
            6. `evidences`

            其中：

            * `independent_question` 是本轮真正需要解决的问题。
            * `original_question` 用于理解用户原始表达和指代关系。
            * `conversation_memory` 用于恢复当前对话上下文。
            * `long_term_memory.exact` 可以作为用户背景、技术栈和偏好信息。
            * `long_term_memory.semantic` 只有在与当前问题明显相关时才能使用。
            * `evidences` 是工具检索或知识库返回的事实依据，应优先用于支撑最终结论。

            长期记忆只能用于理解用户背景和调整回答方式，不能把未经验证的长期记忆当作外部事实证据。

            ## 三、状态枚举约束

            `react_control.status` 只能是：

            ```text
            RUNNING
            COMPLETED
            FAILED
            ```

            `react_control.stop_reason` 只能是：

            ```text
            FINAL_ANSWER
            LIMIT_REACHED
            NEED_USER_INPUT
            ERROR
            null
            ```

            必须遵守以下组合规则：

            | status    | stop_reason     | 含义               |
            | --------- | --------------- | ---------------- |
            | RUNNING   | null            | 流程继续执行           |
            | COMPLETED | FINAL_ANSWER    | 已具备生成最终答案的条件     |
            | FAILED    | LIMIT_REACHED   | 达到迭代次数或工具调用次数上限  |
            | FAILED    | NEED_USER_INPUT | 缺少必须由用户提供的信息     |
            | FAILED    | ERROR           | 输入异常、状态异常或无法继续执行 |

            禁止输出其他组合。

            例如：

            * `RUNNING + FINAL_ANSWER` 非法。
            * `COMPLETED + null` 非法。
            * `FAILED + FINAL_ANSWER` 非法。
            * `RUNNING + NEED_USER_INPUT` 非法。

            ## 四、核心决策流程

            按照以下顺序进行判断。

            ### 第一步：检查终态

            如果当前 `status` 已经是 `COMPLETED` 或 `FAILED`：

            * 不再发起工具调用；
            * 不再修改终止原因；
            * 返回 `NO_OP`；
            * 保持当前状态不变。

            ### 第二步：检查输入有效性

            出现以下情况时，将流程标记为失败：

            * `independent_question` 和 `original_question` 都为空；
            * `react_control` 缺少必要字段；
            * 数值字段出现非法值，例如负数；
            * `iteration > max_iterations`；
            * `tool_call_count > max_tool_calls`；
            * 状态字段不符合枚举约束；
            * 输入结构严重损坏，无法继续分析。

            此时设置：

            ```json
            {
              "status": "FAILED",
              "stop_reason": "ERROR"
            }
            ```

            ### 第三步：判断是否已经具备回答条件

            满足以下条件之一时，可以结束 ReAct 循环并进入最终回答节点：

            1. 已有证据足以支持一个准确、完整、可执行的答案；
            2. 问题属于稳定的通用知识，不依赖实时数据、特定文档或外部验证；
            3. 当前问题主要需要分析、解释或给出排查方法，不需要额外事实；
            4. 已有证据虽然不多，但足以明确说明结论、限制和不确定性；
            5. 继续调用工具预计不会显著提升答案质量。

            此时设置：

            ```json
            {
              "status": "COMPLETED",
              "stop_reason": "FINAL_ANSWER"
            }
            ```

            `next_action` 设置为：

            ```text
            FINAL_ANSWER
            ```

            不要在当前节点输出最终答案正文，只说明已经具备回答条件。

            ### 第四步：判断是否需要用户补充信息

            只有当缺少的信息必须由用户本人提供，并且无法通过工具、上下文或合理假设获得时，才请求用户输入。

            典型情况包括：

            * 缺少具体错误日志；
            * 缺少关键配置代码；
            * 缺少软件版本；
            * 缺少用户期望的业务行为；
            * 问题存在多个完全不同的含义，无法安全推断；
            * 需要访问用户未提供的私有数据。

            此时设置：

            ```json
            {
              "status": "FAILED",
              "stop_reason": "NEED_USER_INPUT"
            }
            ```

            `next_action` 设置为：

            ```text
            ASK_USER
            ```

            同时生成一个简短、具体、一次只询问一个核心信息的问题。

            不得因为一般性的细节不足就请求用户补充信息。能够通过合理假设继续时，应继续执行，并在最终答案中标明假设。

            ### 第五步：检查执行限制

            在决定调用工具前，必须检查：

            ```text
            iteration >= max_iterations
            ```

            或：

            ```text
            tool_call_count >= max_tool_calls
            ```

            只要任意一个条件成立，就不能继续调用工具。

            此时设置：

            ```json
            {
              "status": "FAILED",
              "stop_reason": "LIMIT_REACHED"
            }
            ```

            `next_action` 设置为：

            ```text
            STOP
            ```

            即使证据不完整，也应保留已有证据，供后续节点生成一个带限制说明的最佳努力答案。

            ### 第六步：决定是否调用工具

            仅在以下条件同时满足时调用工具：

            1. 当前证据不足以可靠回答问题；
            2. 缺失信息可以通过工具获得；
            3. 工具返回结果预计会明显提高答案准确性；
            4. 尚未达到迭代上限；
            5. 尚未达到工具调用上限；
            6. 不会重复已经执行过的相同查询；
            7. 不会重复访问已经访问过且没有新增价值的内容。

            此时保持：

            ```json
            {
              "status": "RUNNING",
              "stop_reason": null
            }
            ```

            `next_action` 设置为：

            ```text
            TOOL_CALL
            ```

            并生成一个明确、单一、可执行的工具查询。

            ## 五、工具查询生成规则

            生成工具查询时必须遵守：

            1. 查询应围绕 `independent_question`；
            2. 结合已有证据，只检索当前缺失的信息；
            3. 查询必须具体，避免宽泛搜索；
            4. 不得与 `used_queries` 中的查询完全相同；
            5. 应避免只是改写同一个查询；
            6. 不得请求已经存在于 `evidences` 中的信息；
            7. 不得访问 `visited_chunk_ids` 中已确认无新增价值的内容；
            8. 每轮最多生成一个工具调用请求；
            9. 查询中可以包含技术栈、版本、错误现象和目标行为；
            10. 不要把用户长期记忆中的无关信息放入查询。

            错误示例：

            ```text
            搜索 Spring AI
            ```

            更好的示例：

            ```text
            检索 Spring AI 与 Elasticsearch 集成时，影响向量相似度检索准确率的配置项，包括文档切片、topK、similarityThreshold、向量模型一致性和混合检索配置
            ```

            ## 六、重复检索控制

            根据以下字段避免无效循环：

            ```json
            {
              "used_queries": [],
              "visited_chunk_ids": [],
              "consecutive_no_new_evidence": 0
            }
            ```

            规则如下：

            * 查询已存在于 `used_queries` 时，不得再次使用。
            * 新查询与历史查询语义高度相似，并且没有新的检索方向时，不得再次调用。
            * 已访问的 chunk 不应重复处理，除非需要补充其上下文。
            * 如果 `consecutive_no_new_evidence >= 2`，原则上不应继续检索。
            * 如果连续检索没有新增证据，应优先使用已有信息生成最佳努力答案。
            * 如果连续无新增证据且已接近限制，应结束流程，而不是机械地消耗剩余次数。

            如果已有信息足以提供排查建议，只是无法确认某个具体环境问题，也可以结束检索，并在最终答案中明确需要用户自行验证的部分。

            ## 七、计数器更新责任

            当前节点是 Thought 节点，只负责决策。

            因此：

            * 每完成一次新的 Thought 决策，`iteration` 增加 1；
            * `tool_call_count` 不在 Thought 节点增加；
            * 只有工具真正被调用后，Action 或 Observation 节点才能增加 `tool_call_count`；
            * `used_queries` 只有在工具实际执行后才能追加；
            * `visited_chunk_ids` 只有在工具实际返回并处理结果后才能追加；
            * `consecutive_no_new_evidence` 由 Observation 节点根据检索结果更新；
            * 不得提前假设工具调用成功。

            新的迭代值不得超过 `max_iterations`。

            如果本次增加后将达到迭代上限，但仍需要调用工具，可以允许最后一次工具调用；下一轮不得再次调用工具。

            ## 八、证据充分性判断

            判断证据是否充分时，不要只看证据数量，应看证据是否覆盖回答所需的关键维度。

            对于技术排查问题，通常检查以下维度：

            * 问题现象；
            * 可能原因；
            * 配置位置；
            * 排查顺序；
            * 可执行修改；
            * 验证修改是否有效的方法；
            * 已知限制和版本差异。

            对于“向量检索结果不准确”这类问题，可能涉及：

            * 文档切片大小；
            * chunk overlap；
            * 文档清洗；
            * embedding 模型；
            * 查询和文档是否使用相同 embedding 模型；
            * 向量维度是否一致；
            * Elasticsearch 映射；
            * 相似度算法；
            * topK；
            * similarity threshold；
            * metadata filter；
            * 混合检索；
            * 查询改写；
            * rerank；
            * 数据更新和索引重建；
            * 评估集和召回率验证。

            上述内容只是排查维度，不代表必须全部检索。已有知识足以形成系统性排查方案时，可以直接结束 ReAct 循环。

            ## 九、输出要求

            禁止输出完整思维链、逐步心理推理或冗长分析过程。

            只输出一个合法 JSON 对象，不要输出 Markdown，不要添加解释文字，不要使用代码块。

            输出结构必须是：

            ```json
            {
              "thought_summary": "简短说明当前判断，不超过120个中文字符",
              "evidence_sufficient": true,
              "next_action": "FINAL_ANSWER",
              "tool_request": null,
              "user_question": null,
              "state_patch": {
                "react_control": {
                  "status": "COMPLETED",
                  "iteration": 1,
                  "tool_call_count": 0,
                  "used_queries": [],
                  "visited_chunk_ids": [],
                  "consecutive_no_new_evidence": 0,
                  "stop_reason": "FINAL_ANSWER"
                }
              }
            }
            ```

            `next_action` 只能是：

            ```text
            FINAL_ANSWER
            TOOL_CALL
            ASK_USER
            STOP
            NO_OP
            ```

            ### 当 next_action 为 FINAL_ANSWER

            ```json
            {
              "thought_summary": "已有信息足以生成完整的排查方案，无需继续检索",
              "evidence_sufficient": true,
              "next_action": "FINAL_ANSWER",
              "tool_request": null,
              "user_question": null,
              "state_patch": {
                "react_control": {
                  "status": "COMPLETED",
                  "iteration": 1,
                  "tool_call_count": 0,
                  "used_queries": [],
                  "visited_chunk_ids": [],
                  "consecutive_no_new_evidence": 0,
                  "stop_reason": "FINAL_ANSWER"
                }
              }
            }
            ```

            ### 当 next_action 为 TOOL_CALL

            ```json
            {
              "thought_summary": "缺少当前配置和检索参数相关证据，需要进行一次针对性检索",
              "evidence_sufficient": false,
              "next_action": "TOOL_CALL",
              "tool_request": {
                "query": "具体且不重复的检索查询",
                "purpose": "说明本次查询需要补充的证据",
                "exclude_chunk_ids": []
              },
              "user_question": null,
              "state_patch": {
                "react_control": {
                  "status": "RUNNING",
                  "iteration": 1,
                  "tool_call_count": 0,
                  "used_queries": [],
                  "visited_chunk_ids": [],
                  "consecutive_no_new_evidence": 0,
                  "stop_reason": null
                }
              }
            }
            ```

            注意：工具尚未实际执行，因此不能增加 `tool_call_count`，也不能提前把查询加入 `used_queries`。

            ### 当 next_action 为 ASK_USER

            ```json
            {
              "thought_summary": "缺少必须由用户提供的实际配置，无法准确定位问题",
              "evidence_sufficient": false,
              "next_action": "ASK_USER",
              "tool_request": null,
              "user_question": "请提供当前 VectorStore 查询代码以及 topK 和相似度阈值配置。",
              "state_patch": {
                "react_control": {
                  "status": "FAILED",
                  "iteration": 1,
                  "tool_call_count": 0,
                  "used_queries": [],
                  "visited_chunk_ids": [],
                  "consecutive_no_new_evidence": 0,
                  "stop_reason": "NEED_USER_INPUT"
                }
              }
            }
            ```

            ### 当 next_action 为 STOP

            ```json
            {
              "thought_summary": "已经达到工具调用或迭代次数上限，使用现有证据生成最佳努力答案",
              "evidence_sufficient": false,
              "next_action": "STOP",
              "tool_request": null,
              "user_question": null,
              "state_patch": {
                "react_control": {
                  "status": "FAILED",
                  "iteration": 3,
                  "tool_call_count": 2,
                  "used_queries": [],
                  "visited_chunk_ids": [],
                  "consecutive_no_new_evidence": 0,
                  "stop_reason": "LIMIT_REACHED"
                }
              }
            }
            ```

            ### 当 next_action 为 NO_OP

            ```json
            {
              "thought_summary": "当前状态已经是终态，不再执行任何操作",
              "evidence_sufficient": false,
              "next_action": "NO_OP",
              "tool_request": null,
              "user_question": null,
              "state_patch": {
                "react_control": {
                  "status": "FAILED",
                  "iteration": 3,
                  "tool_call_count": 2,
                  "used_queries": [],
                  "visited_chunk_ids": [],
                  "consecutive_no_new_evidence": 0,
                  "stop_reason": "LIMIT_REACHED"
                }
              }
            }
            ```

            ## 十、重要约束

            * 不得修改用户问题的真实意图。
            * 不得伪造证据、工具结果、配置项或错误日志。
            * 不得把语义记忆当作已经验证的事实。
            * 不得为了使用完迭代次数而继续调用工具。
            * 不得重复执行没有新增价值的查询。
            * 不得在信息已经充分时继续检索。
            * 不得在工具仍可解决问题时过早请求用户输入。
            * 不得输出枚举之外的状态值。
            * 不得输出非法的 `status` 和 `stop_reason` 组合。
            * 不得直接生成最终答案正文。
            * 输出必须是严格合法、可被程序反序列化的 JSON。

            """;

    private static final String NATIVE_REACT_CONTROLLER_SYSTEM_PROMPT = """
            你是知识社区 ReAct 智能体的决策控制器。
            输入是后端维护的完整 AgentState。你只负责判断证据是否充分，不直接回答用户。

            决策规则：
            1. 只有缺少的信息能由已注册工具获得，并且结果会明显改善答案时，才调用工具。
            2. 每轮最多调用一个工具；查询必须具体，不得重复 used_tool_calls 或已有 evidences。
            3. search_article_chunks 用于检索当前文章原文；get_author_articles 仅用于查询当前文章作者的作品列表。
            4. 不得把长期记忆当作已验证的外部事实。
            5. 工具参数中的查询只描述缺失证据，不得包含最终答案。
            6. 需要工具时必须发起模型原生工具调用，不得输出 TOOL_CALL JSON。
            7. 不需要工具时只输出以下三种合法 JSON 之一，不得输出 Markdown、解释或答案正文：

            {"next_action":"FINAL_ANSWER","user_question":null}
            {"next_action":"ASK_USER","user_question":"一个简短、具体且必须由用户回答的问题"}
            {"next_action":"STOP","user_question":null}

            只有缺少的信息必须由用户本人提供且工具、上下文和合理假设都无法获得时，才使用 ASK_USER。
            已有信息足以回答或继续调用工具价值很低时使用 FINAL_ANSWER。
            达到输入状态中的限制时使用 STOP。
            """;

    private static final String FINAL_ANSWER_SYSTEM_PROMPT = """
            你是知识社区问答助手。请根据给定的完整 AgentState 回答用户问题。
            优先回答 independent_question，并结合 conversation_memory 理解上下文。
            long_term_memory 仅用于个性化背景；evidences 是工具返回的事实依据，可能来自文章原文或作者作品列表。
            不得编造证据中不存在的文章内容或作者信息。证据不足时应明确说明限制，并给出可执行的最佳努力答案。
            直接输出面向用户的最终答案，不要输出 AgentState、内部状态、思维链或 JSON。
            """;

    @Autowired
    private VectorStore vectorStore;
    
    @Autowired
    private ChatClient chatClient;

    @Autowired
    private ChatModel chatModel;

    @Autowired
    private ToolCallingManager toolCallingManager;

    @Autowired
    private SummarizingWindowChatMemory chatMemory;

    @Autowired
    private PostMapper postMapper;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @Autowired
    private ArticleSearchTool articleSearchTool;

    @Autowired
    private AuthorArticlesTool authorArticlesTool;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Long createConversation(Long userId, Long postId) {
        if (userId == null || postId == null) {
            throw new IllegalArgumentException("userId 和 postId 不能为空");
        }
        if (postMapper.select(postId) == null) {
            throw new IllegalArgumentException("文章不存在: " + postId);
        }

        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement statement = connection.prepareStatement(
                    "insert into `chat` (user_id, post_id) values (?, ?)",
                    Statement.RETURN_GENERATED_KEYS
            );
            statement.setLong(1, userId);
            statement.setLong(2, postId);
            return statement;
        }, keyHolder);

        Number conversationId = keyHolder.getKey();
        if (conversationId == null) {
            throw new IllegalStateException("数据库未返回会话 ID");
        }
        return conversationId.longValue();
    }

    @Override
    public Flux<String> chat(
            Long userId,
            Long conversationId,
            String question,
            Long postId,
            Integer version
    ) {
        List<Message> conversationMemory = chatMemory.get(String.valueOf(conversationId));
        Post post = postMapper.select(postId);
        if (post == null) {
            return Flux.error(new IllegalArgumentException("文章不存在: " + postId));
        }

        String conversation = conversationMemory.isEmpty()
                ? "（无历史对话）"
                : conversationMemory.stream()
                        .map(message -> message.getRole() + ": " + message.getContent())
                        .reduce((left, right) -> left + "\n" + right)
                        .orElse("（无历史对话）");
        String userPrompt = """
                <article-title>
                %s
                </article-title>
                <conversation-history>
                %s
                </conversation-history>
                <current-question>
                %s
                </current-question>
                """.formatted(post.getTitle(), conversation, question);

        String independentQuestion = chatClient.prompt()
                .system(STANDALONE_QUESTION_SYSTEM_PROMPT)
                .user(userPrompt)
                .call()
                .content();
        if (independentQuestion == null || independentQuestion.isBlank()) {
            independentQuestion = question;
        } else {
            independentQuestion = independentQuestion.trim();
        }
        String memoryPlannerUserPrompt = """
                <independent-question>
                %s
                </independent-question>
                <conversation-history>
                %s
                </conversation-history>
                """.formatted(independentQuestion, conversation);

        MemoryRetrievalPlan memoryRetrievalPlan = chatClient.prompt()
                .system(MEMORY_RETRIEVAL_PLANNER_SYSTEM_PROMPT)
                .user(memoryPlannerUserPrompt)
                .call()
                .entity(MemoryRetrievalPlan.class);
        if (memoryRetrievalPlan == null) {
            return Flux.error(new IllegalStateException("大模型未返回长期记忆检索计划"));
        }
        List<ExactLongMemory> exactLongMemories = queryExactLongMemories(
                userId,
                memoryRetrievalPlan.exactMemory()
        );
        List<Document> vectorLongMemories = queryVectorLongMemories(
                userId,
                memoryRetrievalPlan.vectorMemory()
        );
        List<Message> longMemoryMessages = new ArrayList<>(conversationMemory);
        longMemoryMessages.add(new Message(conversationId, version, "user", question));
        rocketMQTemplate.syncSend("get_long_memory", longMemoryMessages);

        String exactMemoryContext = exactLongMemories.isEmpty()
                ? "（无精确长期记忆）"
                : String.join("\n", exactLongMemories.stream()
                        .map(memory -> memory.memoryKey() + ": " + memory.content())
                        .toList());
        String vectorMemoryContext = vectorLongMemories.isEmpty()
                ? "（无语义长期记忆）"
                : String.join("\n", vectorLongMemories.stream()
                        .map(document -> document.getMetadata().get("memoryKey")
                                + ": " + document.getText())
                        .toList());
        String articleSearchContext = """
                <independent-question>
                %s
                </independent-question>
                <conversation-history>
                %s
                </conversation-history>
                <exact-long-memory>
                %s
                </exact-long-memory>
                <vector-long-memory>
                %s
                </vector-long-memory>
                """.formatted(
                independentQuestion,
                conversation,
                exactMemoryContext,
                vectorMemoryContext
        );

        List<ConversationMemoryItem> conversationItems = conversationMemory.stream()
                .map(message -> new ConversationMemoryItem(message.getRole(), message.getContent()))
                .toList();
        List<LongMemoryItem> exactMemoryItems = exactLongMemories.stream()
                .map(memory -> new LongMemoryItem(memory.memoryKey(), memory.content(), null))
                .toList();
        List<LongMemoryItem> semanticMemoryItems = vectorLongMemories.stream()
                .map(document -> new LongMemoryItem(
                        String.valueOf(document.getMetadata().get("memoryKey")),
                        document.getText(),
                        document.getScore()
                ))
                .toList();

        List<Evidence> evidences = new ArrayList<>();
        Set<String> usedToolCalls = new HashSet<>();
        Set<String> visitedEvidenceIds = new HashSet<>();
        String status = "RUNNING";
        String stopReason = null;
        String userQuestion = null;
        int iteration = 0;
        int toolCallCount = 0;
        int consecutiveNoNewEvidence = 0;

        ToolCallback[] toolCallbacks = memoryRetrievalPlan.articleSearchNeeded()
                ? ToolCallbacks.from(articleSearchTool, authorArticlesTool)
                : ToolCallbacks.from(authorArticlesTool);
        Integer effectiveVersion = version == null ? post.getVersion() : version;
        ToolCallingChatOptions toolOptions = ToolCallingChatOptions.builder()
                .toolCallbacks(toolCallbacks)
                .toolContext(Map.of(
                        ArticleSearchTool.POST_ID_CONTEXT_KEY, postId,
                        ArticleSearchTool.VERSION_CONTEXT_KEY, effectiveVersion,
                        ArticleSearchTool.SEARCH_CONTEXT_KEY, articleSearchContext,
                        AuthorArticlesTool.AUTHOR_ID_CONTEXT_KEY, post.getUserId()
                ))
                .internalToolExecutionEnabled(false)
                .build();

        AgentState currentState = buildAgentState(
                question,
                independentQuestion,
                conversationItems,
                exactMemoryItems,
                semanticMemoryItems,
                status,
                iteration,
                toolCallCount,
                usedToolCalls,
                visitedEvidenceIds,
                consecutiveNoNewEvidence,
                stopReason,
                evidences
        );
        Prompt reactPrompt = new Prompt(
                List.of(
                        new SystemMessage(NATIVE_REACT_CONTROLLER_SYSTEM_PROMPT),
                        new UserMessage(toJson(currentState))
                ),
                toolOptions
        );

        while ("RUNNING".equals(status)) {
                // 硬限制只由后端状态判断，模型不能绕过。
                if (iteration >= MAX_REACT_ITERATIONS
                        || toolCallCount >= MAX_REACT_TOOL_CALLS
                        || consecutiveNoNewEvidence >= MAX_CONSECUTIVE_NO_NEW_EVIDENCE) {
                    status = "FAILED";
                    stopReason = "LIMIT_REACHED";
                    break;
                }

                ChatResponse response;
                try {
                    response = chatModel.call(reactPrompt);
                } catch (RuntimeException exception) {
                    log.error("ReAct 决策调用失败，postId={}，version={}", postId, effectiveVersion, exception);
                    status = "FAILED";
                    stopReason = "ERROR";
                    break;
                }
                iteration++;

                if (response == null || response.getResult() == null || response.getResult().getOutput() == null) {
                    status = "FAILED";
                    stopReason = "ERROR";
                    break;
                }

                if (response.hasToolCalls()) {
                    List<org.springframework.ai.chat.messages.AssistantMessage.ToolCall> toolCalls =
                            response.getResult().getOutput().getToolCalls();
                    ToolCallValidation validation = validateToolCall(
                            toolCalls,
                            toolCallCount,
                            usedToolCalls
                    );
                    if (!validation.accepted()) {
                        status = "FAILED";
                        stopReason = validation.stopReason();
                        break;
                    }

                    org.springframework.ai.chat.messages.AssistantMessage.ToolCall toolCall =
                            validation.toolCall();
                    usedToolCalls.add(validation.toolCallKey());

                    ToolExecutionResult executionResult;
                    try {
                        executionResult = toolCallingManager.executeToolCalls(reactPrompt, response);
                        toolCallCount++;
                    } catch (RuntimeException exception) {
                        log.error("ReAct 工具调用失败，toolName={}，postId={}，version={}",
                                toolCall.name(), postId, effectiveVersion, exception);
                        status = "FAILED";
                        stopReason = "ERROR";
                        break;
                    }

                    int newEvidenceCount = appendToolEvidences(
                            executionResult,
                            visitedEvidenceIds,
                            evidences
                    );
                    consecutiveNoNewEvidence = newEvidenceCount == 0
                            ? consecutiveNoNewEvidence + 1
                            : 0;

                    currentState = buildAgentState(
                            question,
                            independentQuestion,
                            conversationItems,
                            exactMemoryItems,
                            semanticMemoryItems,
                            status,
                            iteration,
                            toolCallCount,
                            usedToolCalls,
                            visitedEvidenceIds,
                            consecutiveNoNewEvidence,
                            stopReason,
                            evidences
                    );
                    List<org.springframework.ai.chat.messages.Message> nextMessages =
                            new ArrayList<>(executionResult.conversationHistory());
                    nextMessages.add(new UserMessage("""
                            <agent-state-update>
                            %s
                            </agent-state-update>
                            """.formatted(toJson(currentState))));
                    reactPrompt = new Prompt(nextMessages, toolOptions);
                    continue;
                }

                TerminalDecision decision = parseTerminalDecision(
                        response.getResult().getOutput().getText()
                );
                if (decision == null || decision.nextAction() == null) {
                    status = "FAILED";
                    stopReason = "ERROR";
                    break;
                }

                switch (decision.nextAction()) {
                    case "FINAL_ANSWER" -> {
                        status = "COMPLETED";
                        stopReason = "FINAL_ANSWER";
                    }
                    case "ASK_USER" -> {
                        status = "FAILED";
                        stopReason = "NEED_USER_INPUT";
                        userQuestion = decision.userQuestion();
                    }
                    case "STOP" -> {
                        status = "FAILED";
                        stopReason = "LIMIT_REACHED";
                    }
                    default -> {
                        status = "FAILED";
                        stopReason = "ERROR";
                    }
                }
            }

        AgentState finalState = buildAgentState(
                question,
                independentQuestion,
                conversationItems,
                exactMemoryItems,
                semanticMemoryItems,
                status,
                iteration,
                toolCallCount,
                usedToolCalls,
                visitedEvidenceIds,
                consecutiveNoNewEvidence,
                stopReason,
                evidences
        );
        if ("NEED_USER_INPUT".equals(stopReason)
                && userQuestion != null
                && !userQuestion.isBlank()) {
            return saveConversation(
                    Flux.just(userQuestion),
                    conversationId,
                    effectiveVersion,
                    question
            );
        }
        return saveConversation(
                chatClient.prompt()
                        .system(FINAL_ANSWER_SYSTEM_PROMPT)
                        .user(toJson(finalState))
                        .stream()
                        .content(),
                conversationId,
                effectiveVersion,
                question
        );
    }

    private List<ExactLongMemory> queryExactLongMemories(Long userId, ExactMemoryPlan plan) {
        if (plan == null) {
            return List.of();
        }

        List<String> memoryKeys = new ArrayList<>(2);
        if (plan.techStack()) {
            memoryKeys.add("tech_stack");
        }
        if (plan.skillLevel() != null && plan.skillLevel().needed()) {
            memoryKeys.add("skill_level");
        }
        if (memoryKeys.isEmpty()) {
            return List.of();
        }

        String placeholders = String.join(", ", java.util.Collections.nCopies(memoryKeys.size(), "?"));
        List<Object> parameters = new ArrayList<>(memoryKeys.size() + 2);
        parameters.add(userId);
        parameters.add("ACTIVE");
        parameters.addAll(memoryKeys);

        return jdbcTemplate.query(
                """
                select id, memory_key, content
                from long_memory
                where user_id = ?
                  and status = ?
                  and memory_key in (%s)
                order by memory_key, id
                """.formatted(placeholders),
                (resultSet, rowNum) -> new ExactLongMemory(
                        resultSet.getLong("id"),
                        resultSet.getString("memory_key"),
                        resultSet.getString("content")
                ),
                parameters.toArray()
        );
    }

    private List<Document> queryVectorLongMemories(Long userId, VectorMemoryPlan plan) {
        if (plan == null) {
            return List.of();
        }

        List<Document> memories = new ArrayList<>(4);
        addVectorLongMemories(memories, userId, "topics", plan.topics());
        addVectorLongMemories(memories, userId, "current_goal", plan.currentGoal());
        return memories;
    }

    private void addVectorLongMemories(
            List<Document> memories,
            Long userId,
            String memoryKey,
            VectorQueryPlan queryPlan
    ) {
        if (queryPlan == null || !queryPlan.needed()) {
            return;
        }
        if (queryPlan.query() == null || queryPlan.query().isBlank()) {
            log.warn("跳过查询内容为空的长期记忆向量检索，userId={}，memoryKey={}", userId, memoryKey);
            return;
        }

        int topK = queryPlan.topK() > 0 ? queryPlan.topK() : 2;
        FilterExpressionBuilder filterBuilder = new FilterExpressionBuilder();
        List<Document> matches = vectorStore.similaritySearch(
                SearchRequest.builder()
                        .query(queryPlan.query())
                        .topK(topK)
                        .filterExpression(
                                filterBuilder.and(
                                        filterBuilder.and(
                                                filterBuilder.eq("userId", userId),
                                                filterBuilder.eq("memoryKey", memoryKey)
                                        ),
                                        filterBuilder.eq("status", "ACTIVE")
                                ).build()
                        )
                        .build()
        );

        memories.addAll(matches.stream()
                .filter(document -> document.getScore() != null
                        && document.getScore() >= LONG_MEMORY_SIMILARITY_THRESHOLD)
                .toList());
    }

    public void saveVectorStore(String content,Integer version,long postId) {
        if (content == null || content.isBlank()) {
            throw new IllegalArgumentException("待向量化的内容不能为空");
        }

        List<String> chunks = chunkMarkdown(content);

        // 组装 Document（文本 + 业务元数据），用于向量写入与检索过滤
        List<Document> docs = new ArrayList<>(chunks.size());
        for (int i = 0; i < chunks.size(); i++) {
            String cid = postId + "#" + i;
            Map<String, Object> meta = new HashMap<>();
            meta.put("postId", String.valueOf(postId));
            meta.put("version", String.valueOf(version));
            meta.put("chunkId", cid);
            docs.add(new Document(chunks.get(i), meta));
        }
        try {
            vectorStore.add(docs);
        } catch (Exception e) {

            throw new RuntimeException("VectorStore add failed", e);
        }
    }

    private List<String> chunkMarkdown(String text) {
        List<String> paras = new ArrayList<>();
        String[] lines = text.split("\r?\n");
        StringBuilder buf = new StringBuilder();
        for (String line : lines) {
            boolean isHeader = line.startsWith("#");
            if (isHeader && !buf.isEmpty()) { // 遇到新的标题，收束上一段
                paras.add(buf.toString());
                buf.setLength(0);
            }
            buf.append(line).append('\n');
        }
        if (!buf.isEmpty()) paras.add(buf.toString());

        return getChunks(paras);
    }
    /**
     * 固定长度切片（每片 ≤ 800 字符），切片间 100 字符重叠：
     * - 兼顾检索召回与上下文连续性
     */
    private static List<String> getChunks(List<String> paras) {
        List<String> chunks = new ArrayList<>();
        for (String p : paras) {
            if (p.length() <= 800) {
                chunks.add(p);
            } else {
                int start = 0;
                while (start < p.length()) {
                    int end = Math.min(start + 800, p.length());
                    chunks.add(p.substring(start, end));
                    if (end >= p.length()) break;
                    start = Math.max(end - 100, start + 1); // 重叠 100 字符以保留语义连续
                }
            }
        }
        return chunks;
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("AgentState JSON 序列化失败", exception);
        }
    }

    private AgentState buildAgentState(
            String originalQuestion,
            String independentQuestion,
            List<ConversationMemoryItem> conversationItems,
            List<LongMemoryItem> exactMemoryItems,
            List<LongMemoryItem> semanticMemoryItems,
            String status,
            int iteration,
            int toolCallCount,
            Set<String> usedToolCalls,
            Set<String> visitedEvidenceIds,
            int consecutiveNoNewEvidence,
            String stopReason,
            List<Evidence> evidences
    ) {
        return new AgentState(
                originalQuestion,
                independentQuestion,
                conversationItems,
                new LongTermMemoryState(exactMemoryItems, semanticMemoryItems),
                new ReactControl(
                        status,
                        iteration,
                        MAX_REACT_ITERATIONS,
                        toolCallCount,
                        MAX_REACT_TOOL_CALLS,
                        List.copyOf(usedToolCalls),
                        List.copyOf(visitedEvidenceIds),
                        consecutiveNoNewEvidence,
                        stopReason
                ),
                List.copyOf(evidences)
        );
    }

    private ToolCallValidation validateToolCall(
            List<org.springframework.ai.chat.messages.AssistantMessage.ToolCall> toolCalls,
            int toolCallCount,
            Set<String> usedToolCalls
    ) {
        if (toolCalls == null
                || toolCalls.size() != 1
                || toolCallCount >= MAX_REACT_TOOL_CALLS) {
            return new ToolCallValidation(null, null, "LIMIT_REACHED");
        }
        org.springframework.ai.chat.messages.AssistantMessage.ToolCall toolCall = toolCalls.get(0);
        String toolCallKey = canonicalToolCallKey(toolCall.name(), toolCall.arguments());
        if (usedToolCalls.contains(toolCallKey)) {
            return new ToolCallValidation(null, null, "ERROR");
        }
        return new ToolCallValidation(toolCall, toolCallKey, null);
    }

    private String canonicalToolCallKey(String toolName, String arguments) {
        String canonicalArguments = arguments == null ? "" : arguments;
        if (arguments != null && !arguments.isBlank()) {
            try {
                canonicalArguments = objectMapper.writeValueAsString(objectMapper.readTree(arguments));
            } catch (JsonProcessingException exception) {
                log.warn("工具参数不是合法 JSON，将使用原始参数进行去重，toolName={}", toolName);
            }
        }
        return toolName + ":" + canonicalArguments;
    }

    private int appendToolEvidences(
            ToolExecutionResult executionResult,
            Set<String> visitedEvidenceIds,
            List<Evidence> evidences
    ) {
        int added = 0;
        for (org.springframework.ai.chat.messages.Message message : executionResult.conversationHistory()) {
            if (!(message instanceof ToolResponseMessage toolResponseMessage)) {
                continue;
            }
            for (ToolResponseMessage.ToolResponse response : toolResponseMessage.getResponses()) {
                String responseData = response.responseData();
                if (responseData == null || responseData.isBlank()) {
                    continue;
                }
                try {
                    JsonNode root = objectMapper.readTree(responseData);
                    if (root.isArray()) {
                        for (JsonNode item : root) {
                            added += addEvidence(
                                    response.name(),
                                    item.isTextual() ? item.asText() : item.toString(),
                                    visitedEvidenceIds,
                                    evidences
                            );
                        }
                    } else {
                        added += addEvidence(
                                response.name(),
                                root.isTextual() ? root.asText() : root.toString(),
                                visitedEvidenceIds,
                                evidences
                        );
                    }
                } catch (JsonProcessingException exception) {
                    added += addEvidence(
                            response.name(),
                            responseData,
                            visitedEvidenceIds,
                            evidences
                    );
                }
            }
        }
        return added;
    }

    private static int addEvidence(
            String toolName,
            String content,
            Set<String> visitedEvidenceIds,
            List<Evidence> evidences
    ) {
        if (content == null || content.isBlank()) {
            return 0;
        }
        String evidenceId = toolName + "#"
                + Integer.toUnsignedString((toolName + "\0" + content).hashCode(), 16);
        if (!visitedEvidenceIds.add(evidenceId)) {
            return 0;
        }
        evidences.add(new Evidence(evidenceId, toolName, content));
        return 1;
    }

    private TerminalDecision parseTerminalDecision(String content) {
        if (content == null || content.isBlank()) {
            return null;
        }
        String json = content.trim();
        if (json.startsWith("```")) {
            json = json.replaceFirst("^```(?:json)?\\s*", "")
                    .replaceFirst("\\s*```$", "");
        }
        try {
            return objectMapper.readValue(json, TerminalDecision.class);
        } catch (JsonProcessingException exception) {
            log.warn("ReAct 终态决策无法解析: {}", content);
            return null;
        }
    }

    private Flux<String> saveConversation(
            Flux<String> response,
            Long conversationId,
            Integer version,
            String question
    ) {
        StringBuilder answer = new StringBuilder();
        AtomicBoolean saved = new AtomicBoolean(false);
        return response
                .doOnNext(answer::append)
                .doFinally(signalType -> {
                    if (signalType == SignalType.ON_COMPLETE && saved.compareAndSet(false, true)) {
                        chatMemory.add(
                                String.valueOf(conversationId),
                                List.of(
                                        new Message(conversationId, version, "user", question),
                                        new Message(conversationId, version, "assistant", answer.toString())
                                )
                        );
                    }
                });
    }

    private record AgentState(
            @JsonProperty("original_question") String originalQuestion,
            @JsonProperty("independent_question") String independentQuestion,
            @JsonProperty("conversation_memory") List<ConversationMemoryItem> conversationMemory,
            @JsonProperty("long_term_memory") LongTermMemoryState longTermMemory,
            @JsonProperty("react_control") ReactControl reactControl,
            List<Evidence> evidences
    ) {
    }

    private record ConversationMemoryItem(String role, String content) {
    }

    private record LongTermMemoryState(
            List<LongMemoryItem> exact,
            List<LongMemoryItem> semantic
    ) {
    }

    private record LongMemoryItem(
            @JsonProperty("memory_key") String memoryKey,
            String content,
            Double score
    ) {
    }

    private record ReactControl(
            String status,
            int iteration,
            @JsonProperty("max_iterations") int maxIterations,
            @JsonProperty("tool_call_count") int toolCallCount,
            @JsonProperty("max_tool_calls") int maxToolCalls,
            @JsonProperty("used_tool_calls") List<String> usedToolCalls,
            @JsonProperty("visited_evidence_ids") List<String> visitedEvidenceIds,
            @JsonProperty("consecutive_no_new_evidence") int consecutiveNoNewEvidence,
            @JsonProperty("stop_reason") String stopReason
    ) {
    }

    private record TerminalDecision(
            @JsonProperty("next_action") String nextAction,
            @JsonProperty("user_question") String userQuestion
    ) {
    }

    private record ToolCallValidation(
            org.springframework.ai.chat.messages.AssistantMessage.ToolCall toolCall,
            String toolCallKey,
            String stopReason
    ) {
        private boolean accepted() {
            return toolCall != null;
        }
    }

    private record Evidence(
            @JsonProperty("evidence_id") String evidenceId,
            @JsonProperty("tool_name") String toolName,
            String content
    ) {
    }

    private record MemoryRetrievalPlan(
            @JsonProperty("exact_memory") ExactMemoryPlan exactMemory,
            @JsonProperty("vector_memory") VectorMemoryPlan vectorMemory,
            @JsonProperty("article_search_needed") boolean articleSearchNeeded
    ) {
    }

    private record ExactMemoryPlan(
            @JsonProperty("tech_stack") boolean techStack,
            @JsonProperty("skill_level") NeededPlan skillLevel
    ) {
    }

    private record NeededPlan(boolean needed) {
    }

    private record VectorMemoryPlan(
            VectorQueryPlan topics,
            @JsonProperty("current_goal") VectorQueryPlan currentGoal
    ) {
    }

    private record VectorQueryPlan(
            boolean needed,
            String query,
            @JsonProperty("top_k") int topK
    ) {
    }

    private record ExactLongMemory(
            Long id,
            String memoryKey,
            String content
    ) {
    }
}
