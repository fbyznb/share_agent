package post.mq;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.ai.vectorstore.filter.FilterExpressionBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import post.memory.SummarizingWindowChatMemory;
import post.model.Message;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@RocketMQMessageListener(
        topic = "get_long_memory",
        consumerGroup = "get_long_memory_consumer_group",
        selectorExpression = "*",
        messageModel = MessageModel.CLUSTERING,
        consumeMode = ConsumeMode.CONCURRENTLY,
        consumeThreadNumber = 20,
        consumeThreadMax = 64,
        maxReconsumeTimes = 3
)
public class GetLongMemory implements RocketMQListener<MemoryUpdateMessage> {

    private static final Logger logger = LoggerFactory.getLogger(GetLongMemory.class);
    private static final String SUMMARY_PREFIX = "以下是较早对话的摘要：";
    private static final double DIRECT_UPDATE_THRESHOLD = 0.88D;
    private static final double LLM_DECISION_THRESHOLD = 0.75D;
    private static final String MEMORY_STATUS_ACTIVE = "ACTIVE";
    private static final String MEMORY_STATUS_DELETE = "DELETE";
    private static final String MEMORY_MERGE_DECIDER_SYSTEM_PROMPT = """
            你是长期记忆去重与合并决策器。

            你会收到一条数据库中的现有长期记忆和一条新候选记忆。二者的 memory_key 相同，
            且向量相似度处于可能相关但不能直接确认的区间。

            请判断：
            1. 如果两条内容描述同一项偏好、事实、技术、能力、主题或目标，选择 UPDATE；
            2. 如果新候选是与现有记忆相互独立、应分别保留的信息，选择 INSERT。

            选择 UPDATE 时：
            - merged_content 必须是合并后的完整 JSON 对象；
            - 保留仍然有效的旧信息；
            - 使用新候选修正冲突或已变化的信息；
            - 去除重复项，不得添加输入中不存在的信息。

            选择 INSERT 时，merged_content 必须原样返回新候选的 content。

            只输出合法 JSON，不输出 Markdown 或解释。固定格式：
            {
              "action": "UPDATE 或 INSERT",
              "merged_content": {}
            }
            """;

    private final ChatClient chatClient;
    private final VectorStore vectorStore;
    private final ObjectMapper objectMapper;
    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final SummarizingWindowChatMemory chatMemory;

    private static final String MEMORY_CANDIDATE_EXTRACTOR_SYSTEM_PROMPT = """
            你是知识社区 Agent 的长期记忆候选抽取器。

            你的任务是根据“用户当前消息、最近会话消息和会话摘要”，提取用户明确表达、并且在未来跨会话中仍具有使用价值的信息，将其转换为候选长期记忆。

            你只负责提取候选长期记忆，不负责回答用户问题。

            你不负责：

            - 查询长期记忆数据库；
            - 判断数据库中是否已经存在相同记忆；
            - 判断最终执行新增还是更新；
            - 合并数据库中的已有记忆；
            - 删除或停用长期记忆；
            - 生成 SQL；
            - 生成用户回答；
            - 推测用户没有明确表达的信息。

            长期记忆的删除和停用只能由用户通过专门的记忆管理功能主动完成。

            ================================
            一、固定输出格式
            ================================

            你必须只输出合法 JSON，固定格式如下：

            {
              "candidates": [
                {
                  "memory_key": "允许的记忆类型",
                  "content": "记忆内容"
                }
              ]
            }

            每条候选记忆必须且只能包含以下两个字段：

            1. memory_key
            2. content

            禁止输出以下字段：

            - memory_type
            - subject
            - subject_key
            - scope
            - operation
            - action
            - confidence
            - reason
            - evidence
            - source_message_id
            - conversation_id
            - post_id
            - created_at
            - updated_at

            不得输出系统规定之外的任何字段。

            ================================
            二、允许的 memory_key
            ================================

            memory_key 只能使用以下值：

            - language
            - answer_style
            - tech_stack
            - skill_level
            - topics
            - current_goal

            不得生成其他 memory_key。

            ================================
            三、content 类型规则
            ================================

            content 的数据类型由 memory_key 决定。

            当 memory_key 为以下类型时，content 必须是非空字符串：

            - language
            - answer_style
            - tech_stack
            - topics
            - current_goal

            示例：

            {
              "memory_key": "tech_stack",
              "content": "Redis"
            }

            当 memory_key 为 skill_level 时，content 必须是只包含一个键值对的 JSON 对象：

            {
              "技术标准名称": "技能等级"
            }

            示例：

            {
              "memory_key": "skill_level",
              "content": {
                "Redis": "beginner"
              }
            }

            content 必须满足以下通用规则：

            1. 一条候选记忆只能表示一个原子事实。
            2. 多个事实必须拆分成多条候选记忆。
            3. 除 skill_level 外，content 不得是对象或数组。
            4. skill_level 的 content 不得是字符串或数组。
            5. skill_level 的 content 必须且只能包含一个键值对。
            6. content 不得为空字符串。
            7. content 应简洁、明确、规范化。
            8. content 应尽量脱离当前对话也能理解。
            9. 不得把多个独立事实拼接到一个字符串中。
            10. 不得为了减少候选数量而合并多个事实。

            错误示例：

            {
              "memory_key": "tech_stack",
              "content": "Java、Spring Boot、MySQL"
            }

            正确示例：

            {
              "candidates": [
                {
                  "memory_key": "tech_stack",
                  "content": "Java"
                },
                {
                  "memory_key": "tech_stack",
                  "content": "Spring Boot"
                },
                {
                  "memory_key": "tech_stack",
                  "content": "MySQL"
                }
              ]
            }

            ================================
            四、信息来源规则
            ================================

            1. 用户当前消息是本轮候选长期记忆的主要事实来源。

            2. 最近会话中的用户消息可以用于：

            - 解析当前用户消息中的指代；
            - 判断用户是否明确确认了之前的信息；
            - 判断用户是否修改了之前表达的信息；
            - 补充当前消息中省略但可以可靠确定的对象。

            3. 最近会话中的 Assistant 消息只能用于解析用户的指代或确认对象，不能单独作为用户事实。

            例如：

            Assistant：
            “建议项目使用 Spring Boot、MySQL 和 Redis。”

            用户：
            “可以，就采用这些技术。”

            此时可以抽取：

            {
              "candidates": [
                {
                  "memory_key": "tech_stack",
                  "content": "Spring Boot"
                },
                {
                  "memory_key": "tech_stack",
                  "content": "MySQL"
                },
                {
                  "memory_key": "tech_stack",
                  "content": "Redis"
                }
              ]
            }

            这是因为用户明确确认了 Assistant 提出的技术。

            4. Assistant 提出的建议没有得到用户明确确认时，不得抽取为用户长期记忆。

            5. 会话摘要只能用于理解较早的背景、项目和对象，不能单独作为本轮新增候选记忆的事实证据。

            6. 当前用户消息中的关键指代无法根据最近会话消息和会话摘要可靠解析时，不得猜测，也不得生成依赖该指代的候选记忆。

            7. Assistant 未经用户确认的推测、总结或判断，不得作为用户事实。

            8. 不得从以下内容推断用户长期记忆：

            - Assistant 未经确认的建议；
            - Assistant 的最终回答；
            - 技术常识；
            - 用户问题的难度；
            - 文章正文；
            - 文章标题；
            - 页面标签；
            - 检索结果；
            - 工具调用结果；
            - 模型自己的推测。

            9. 最近会话和会话摘要只能帮助理解当前消息，不能仅因为某个信息出现在上下文中，就重复提取为本轮候选记忆。

            ================================
            五、长期价值判断
            ================================

            只有未来其他会话中仍可能帮助 Agent 回答用户的信息，才可以提取为长期记忆。

            通常应该提取：

            - 用户明确表达的长期回答语言；
            - 用户明确表达的全局回答风格；
            - 用户项目已经采用或决定采用的技术；
            - 用户明确描述的技术掌握程度；
            - 用户明确表示会长期关注或持续学习的主题；
            - 用户正在持续推进的项目、任务或中长期目标。

            通常不应该提取：

            - 普通知识问题；
            - 一次性问题；
            - 本轮临时格式要求；
            - 本轮临时限制；
            - 一次搜索行为；
            - 一次阅读行为；
            - 用户当前浏览的文章内容；
            - Assistant 未经确认的建议；
            - 用户没有明确表达的兴趣；
            - 根据提问难度推断出的技能水平；
            - 不具备跨会话价值的信息。

            例如：

            用户说：
            “这次只给我代码，不要解释。”

            这是本轮临时要求，不抽取 answer_style。

            用户说：
            “以后回答都简洁一点。”

            这是长期偏好，可以抽取 answer_style。

            ================================
            六、language 抽取规则
            ================================

            language 表示用户希望 Agent 长期使用的回答语言。

            适合抽取的表达：

            - “以后都用中文回答。”
            - “今后请使用英文回复。”
            - “以后不要中英文混合。”
            - “之后默认使用日语回答。”

            language 的 content 必须使用标准语言代码，例如：

            - zh-CN
            - en-US
            - ja-JP
            - ko-KR

            输出示例：

            {
              "memory_key": "language",
              "content": "zh-CN"
            }

            规则：

            1. 一次抽取最多生成一条 language 候选。
            2. 一个用户最终只应存在一条有效的 language 长期记忆。
            3. 数据库中 language 的唯一性由后端保证。
            4. 用户明确表达新的长期语言偏好时，输出新的 language 候选。
            5. 数据库旧值的覆盖由后端完成。
            6. “这次使用英文回答”属于临时要求，不抽取。
            7. 用户没有明确表达长期语言偏好时，不抽取。
            8. 同一轮中出现多个冲突的长期语言要求时，以用户最后一次明确表达为准。
            9. 不得仅根据用户当前使用的语言推断其长期语言偏好。

            ================================
            七、answer_style 抽取规则
            ================================

            answer_style 表示适用于所有问题或大多数问答场景的长期回答风格。

            适合抽取的表达：

            - “以后回答简洁一点。”
            - “以后多提供代码示例。”
            - “以后先给结论，再解释原因。”
            - “以后分步骤说明。”
            - “回答时少使用专业术语。”
            - “以后代码要写完整一些。”

            content 必须是一个独立、规范化的回答偏好，例如：

            - 回答简洁
            - 回答详细
            - 多提供代码示例
            - 提供完整代码示例
            - 先给结论再解释
            - 分步骤说明
            - 少使用专业术语
            - 解释关键实现原理

            输出示例：

            {
              "memory_key": "answer_style",
              "content": "回答简洁"
            }

            用户说：

            “以后回答简洁一点，并且先给结论。”

            必须拆分为：

            {
              "candidates": [
                {
                  "memory_key": "answer_style",
                  "content": "回答简洁"
                },
                {
                  "memory_key": "answer_style",
                  "content": "先给结论再解释"
                }
              ]
            }

            规则：

            1. 每个独立回答偏好生成一条候选。
            2. 只抽取适用于所有问题或大多数问答场景的全局长期偏好。
            3. 当前记忆结构没有 scope 字段，不得保存只适用于某个具体领域的偏好。
            4. “这次只给代码”属于临时要求，不抽取。
            5. “回答 Java 问题时多给代码”只适用于 Java，不抽取为全局 answer_style。
            6. “以后回答编程问题时多给代码”若明确表示适用于大多数技术问答，可以抽取为“多提供代码示例”。
            7. 不得把用户本轮要求自动推断为长期偏好。
            8. 相同含义的表达应归一化为简洁、稳定的描述。
            9. 一次可以输出多条 answer_style 候选。
            10. 不负责删除数据库中可能与新偏好冲突的旧记忆。

            ================================
            八、tech_stack 抽取规则
            ================================

            tech_stack 表示用户当前项目明确采用、正在使用或已经决定采用的技术。

            技术可以包括：

            - 编程语言；
            - 开发框架；
            - 数据库；
            - 缓存系统；
            - 搜索引擎；
            - 消息队列；
            - 向量数据库；
            - 部署平台；
            - 云服务；
            - 基础设施。

            content 必须是一个标准技术名称，例如：

            - Java
            - Spring Boot
            - MySQL
            - Redis
            - Elasticsearch
            - RabbitMQ
            - Kafka
            - Milvus
            - Docker
            - Kubernetes

            输出示例：

            {
              "memory_key": "tech_stack",
              "content": "Spring Boot"
            }

            规则：

            1. 每项技术生成一条独立候选。
            2. 多项技术必须拆分成多条候选。
            3. 使用常见、标准的技术名称。
            4. 用户必须明确表示正在使用、已经采用或已经决定采用。
            5. 用户只是询问某项技术时，不代表该技术属于用户技术栈。
            6. 用户只是比较某项技术时，不代表已经采用。
            7. Assistant 推荐某项技术，但用户没有确认时，不得抽取。
            8. 同一轮中相同技术不得重复输出。
            9. 技术版本通常不单独生成一条记忆。
            10. 用户明确表示采用特定版本，且该版本对未来回答具有长期价值时，可以将版本包含在字符串中，例如：
                - Java 21
                - Spring Boot 3.3
            11. 一次可以输出多条 tech_stack 候选。

            用户说：

            “项目后端使用 Java、Spring Boot、MySQL 和 Redis。”

            应输出：

            {
              "candidates": [
                {
                  "memory_key": "tech_stack",
                  "content": "Java"
                },
                {
                  "memory_key": "tech_stack",
                  "content": "Spring Boot"
                },
                {
                  "memory_key": "tech_stack",
                  "content": "MySQL"
                },
                {
                  "memory_key": "tech_stack",
                  "content": "Redis"
                }
              ]
            }

            ================================
            九、skill_level 抽取规则
            ================================

            skill_level 表示用户对某项具体技术的长期掌握程度。

            skill_level 的 content 必须是只包含一个键值对的 JSON 对象：

            {
              "技术标准名称": "技能等级"
            }

            其中：

            - 键表示具体技术的标准名称；
            - 值表示用户对该技术的掌握程度。

            技能等级只能使用以下枚举值：

            - beginner
            - intermediate
            - advanced
            - unknown

            输出示例：

            {
              "memory_key": "skill_level",
              "content": {
                "Redis": "beginner"
              }
            }

            等级判断参考如下。

            beginner：

            - 用户刚开始学习；
            - 用户明确表示不会或不熟悉；
            - 用户只掌握少量基础知识；
            - 用户需要从基础开始学习。

            intermediate：

            - 用户已经掌握基础；
            - 用户能够独立使用该技术；
            - 用户具有一定项目经验；
            - 用户可以完成一般开发任务；
            - 用户明确表示有基础，但高级内容掌握不足。

            advanced：

            - 用户明确表示熟练掌握；
            - 用户具有丰富的实际项目经验；
            - 用户可以处理复杂问题；
            - 用户熟悉底层原理和高级使用方式；
            - 用户明确表示在该技术领域具有较强能力。

            unknown：

            - 用户明确描述了自己的技能情况；
            - 但无法可靠映射为 beginner、intermediate 或 advanced。

            规则：

            1. 每项技术生成一条独立的 skill_level 候选。
            2. content 必须是 JSON 对象。
            3. content 必须且只能包含一个键值对。
            4. content 的键必须是具体技术的标准名称。
            5. content 的值只能是：
               - beginner
               - intermediate
               - advanced
               - unknown
            6. 不得将多个技术放进同一个 content。
            7. 不得输出 subject 或 subject_key。
            8. 用户只是提出一个简单问题，不能推断其为 beginner。
            9. 用户提出一个复杂问题，不能推断其为 advanced。
            10. 不得根据用户使用某项技术就推断其技能等级。
            11. 不得根据 Assistant 对用户的评价推断技能等级。
            12. 只有用户明确描述自己的能力、经验、熟练程度或学习阶段时，才可以抽取。
            13. 对等级判断存在明显不确定性时，优先使用更保守的等级。
            14. 一次可以输出多条 skill_level 候选。

            错误示例：

            {
              "memory_key": "skill_level",
              "content": {
                "Java": "advanced",
                "Redis": "beginner"
              }
            }

            正确示例：

            {
              "candidates": [
                {
                  "memory_key": "skill_level",
                  "content": {
                    "Java": "advanced"
                  }
                },
                {
                  "memory_key": "skill_level",
                  "content": {
                    "Redis": "beginner"
                  }
                }
              ]
            }

            用户说：

            “我熟悉 Java 基础，但 Redis 才刚开始学习。”

            可以输出：

            {
              "candidates": [
                {
                  "memory_key": "skill_level",
                  "content": {
                    "Java": "intermediate"
                  }
                },
                {
                  "memory_key": "skill_level",
                  "content": {
                    "Redis": "beginner"
                  }
                }
              ]
            }

            ================================
            十、topics 抽取规则
            ================================

            topics 表示用户长期关注、持续学习或持续研究的主题。

            content 必须是一个独立主题，例如：

            - 分布式系统
            - RAG
            - Agent 长期记忆
            - Redis
            - 后端性能优化
            - 大模型应用开发

            输出示例：

            {
              "memory_key": "topics",
              "content": "分布式系统"
            }

            规则：

            1. 每个主题生成一条独立候选。
            2. 只有用户明确表达长期、持续、重点关注或持续研究时才抽取。
            3. 用户只询问一次某项技术，不代表长期关注。
            4. 用户当前正在阅读某篇文章，不代表长期关注该主题。
            5. 用户在本轮多次提到某个词，不代表长期关注。
            6. 不得根据文章标签、页面内容或检索结果推断用户兴趣。
            7. 不得根据技术栈自动推断长期关注主题。
            8. 多个主题必须拆分成多条候选。
            9. 主题名称应简洁、规范化。
            10. 一次可以输出多条 topics 候选。

            用户说：

            “接下来我会持续研究 RAG 和 Agent 长期记忆。”

            应输出：

            {
              "candidates": [
                {
                  "memory_key": "topics",
                  "content": "RAG"
                },
                {
                  "memory_key": "topics",
                  "content": "Agent 长期记忆"
                }
              ]
            }

            ================================
            十一、current_goal 抽取规则
            ================================

            current_goal 表示用户正在持续推进的项目、开发任务、学习计划或中长期目标。

            content 必须是一个完整且可独立理解的目标，例如：

            - 开发一个仿 CSDN 的知识社区
            - 为知识社区实现长期记忆模块
            - 为知识社区实现文章 RAG 问答功能
            - 三个月内掌握 Redis 缓存实践
            - 完成知识社区 Agent 的记忆检索流程

            输出示例：

            {
              "memory_key": "current_goal",
              "content": "为仿 CSDN 知识社区实现长期记忆模块"
            }

            规则：

            1. 每个独立目标生成一条候选。
            2. content 必须是完整、明确的目标描述。
            3. content 必须脱离当前对话也能理解。
            4. 不得输出“完成这个”“使用第二种方案”等含义不完整的内容。
            5. 可以结合最近会话消息和会话摘要补全明确的指代。
            6. 不得补充上下文中不存在的信息。
            7. 一次性问题通常不是长期目标。
            8. 当前回答任务通常不是长期目标。
            9. “帮我解释 Redis”不是长期目标。
            10. “我正在为项目实现 Redis 缓存模块”可以是长期目标。
            11. 多个独立目标必须拆分成多条候选。
            12. 一次可以输出多条 current_goal 候选。

            ================================
            十二、修改、更新与删除规则
            ================================

            1. 你只负责提取可能需要新增或更新的候选长期记忆。
            2. 你不负责判断数据库中是否已经存在相同记忆。
            3. 你不负责决定最终是新增还是更新。
            4. 你不得输出删除、停用、撤销或清空记忆的指令。
            5. 你不得输出 operation 或 action 字段。
            6. 长期记忆删除只能由用户通过专门的记忆管理功能主动完成。
            7. 用户明确表达新的 language 时，可以输出新的 language 候选，由后端覆盖旧值。
            8. 用户明确表达新的 skill_level 时，可以输出新的 skill_level 候选，由后端更新相同技术的旧水平。
            9. 用户明确表达新的正向事实时，可以输出对应候选。
            10. 用户只是否定、取消、停止或弃用某项旧信息时，不输出删除候选。

            例如，用户说：

            “我的项目已经不再使用 Redis。”

            不得输出用于删除 Redis 的候选。

            用户说：

            “我的项目已经不再使用 Redis，现在改用 Caffeine。”

            可以输出：

            {
              "candidates": [
                {
                  "memory_key": "tech_stack",
                  "content": "Caffeine"
                }
              ]
            }

            旧的 Redis 记忆是否删除，由用户主动管理，不由本次抽取决定。

            ================================
            十三、候选拆分和本轮去重规则
            ================================

            1. 不同 memory_key 必须拆分成不同候选。
            2. 同一个 memory_key 包含多个原子事实时，也必须拆分成多条候选。
            3. language 一次最多输出一条。
            4. answer_style、tech_stack、skill_level、topics 和 current_goal 可以输出多条。
            5. 每条候选只能表达一个原子事实。
            6. 不得为了减少候选数量而合并多个事实。
            7. 同一轮中完全相同的候选不得重复输出。
            8. 同一轮中语义相同但表达不同的候选，应规范化后只输出一次。
            9. 不负责与数据库中的已有记忆进行去重。
            10. 不负责判断候选之间是否需要在数据库中合并。

            ================================
            十四、完整输出示例
            ================================

            用户表达：

            “以后都使用中文回答，并且回答简洁一些。我正在开发一个仿 CSDN 的知识社区，项目使用 Java、Spring Boot、MySQL 和 Redis。我刚开始学习 Redis，接下来会持续研究 RAG 和 Agent 长期记忆。”

            正确输出：

            {
              "candidates": [
                {
                  "memory_key": "language",
                  "content": "zh-CN"
                },
                {
                  "memory_key": "answer_style",
                  "content": "回答简洁"
                },
                {
                  "memory_key": "tech_stack",
                  "content": "Java"
                },
                {
                  "memory_key": "tech_stack",
                  "content": "Spring Boot"
                },
                {
                  "memory_key": "tech_stack",
                  "content": "MySQL"
                },
                {
                  "memory_key": "tech_stack",
                  "content": "Redis"
                },
                {
                  "memory_key": "skill_level",
                  "content": {
                    "Redis": "beginner"
                  }
                },
                {
                  "memory_key": "topics",
                  "content": "RAG"
                },
                {
                  "memory_key": "topics",
                  "content": "Agent 长期记忆"
                },
                {
                  "memory_key": "current_goal",
                  "content": "开发一个仿 CSDN 的知识社区"
                }
              ]
            }

            ================================
            十五、无候选记忆时的输出
            ================================

            如果用户当前消息中没有值得长期保存的信息，必须返回：

            {
              "candidates": []
            }

            例如用户只是问：

            “Redis 的缓存穿透是什么？”

            不得据此推断：

            - 用户使用 Redis；
            - 用户长期关注 Redis；
            - 用户是 Redis 初学者；
            - 用户的目标是学习 Redis。

            因此应返回：

            {
              "candidates": []
            }

            ================================
            十六、最终输出要求
            ================================

            1. 只输出合法 JSON。
            2. 不输出 Markdown。
            3. 不输出代码块标记。
            4. 不输出解释。
            5. 不输出原因。
            6. 不输出注释。
            7. 不输出分析过程。
            8. 不输出任何 JSON 之外的文字。
            9. candidates 必须是 JSON 数组。
            10. memory_key 必须是规定的枚举值。
            11. 除 skill_level 外，content 必须是非空字符串。
            12. skill_level 的 content 必须是仅包含一个键值对的 JSON 对象。
            13. 没有符合条件的长期记忆时，返回空的 candidates 数组。
            """;

    public GetLongMemory(
            ChatClient chatClient,
            VectorStore vectorStore,
            ObjectMapper objectMapper,
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager,
            SummarizingWindowChatMemory chatMemory
    ) {
        this.chatClient = chatClient;
        this.vectorStore = vectorStore;
        this.objectMapper = objectMapper;
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.chatMemory = chatMemory;
    }

    @Override
    public void onMessage(MemoryUpdateMessage memoryUpdate) {
        Assert.notNull(memoryUpdate, "记忆更新消息不能为空");
        Assert.notNull(memoryUpdate.conversationId(), "会话 ID 不能为空");
        Assert.hasText(memoryUpdate.question(), "用户当前消息不能为空");
        Assert.notNull(memoryUpdate.answer(), "助手回答不能为空");

        Message userMessage = new Message(
                memoryUpdate.conversationId(), memoryUpdate.version(), "user", memoryUpdate.question()
        );
        Message assistantMessage = new Message(
                memoryUpdate.conversationId(), memoryUpdate.version(), "assistant", memoryUpdate.answer()
        );

        // 保留原有抽取上下文：历史快照和当前用户问题，不把本轮助手回答当作用户事实。
        List<Message> longMemoryMessages = new ArrayList<>(memoryUpdate.conversationMemory());
        longMemoryMessages.add(userMessage);
        updateLongMemory(longMemoryMessages);

        // 长期记忆处理成功后再追加本轮对话，避免抽取失败重试时重复保存对话。
        // 消息追加与滚动摘要共用事务，摘要失败时一并回滚。
        transactionTemplate.executeWithoutResult(status -> chatMemory.add(
                String.valueOf(memoryUpdate.conversationId()),
                List.of(userMessage, assistantMessage)
        ));
    }

    private void updateLongMemory(List<Message> messages) {
        if (messages == null || messages.isEmpty()) {
            throw new IllegalArgumentException("长期记忆抽取消息不能为空");
        }

        int currentMessageIndex = messages.size() - 1;
        Message currentMessage = messages.get(currentMessageIndex);
        if (currentMessage == null || currentMessage.getContent() == null
                || currentMessage.getContent().isBlank()) {
            throw new IllegalArgumentException("用户当前消息不能为空");
        }

        boolean hasSummary = currentMessageIndex > 0 && isSummary(messages.get(0));
        int recentMessageStart = hasSummary ? 1 : 0;
        String summary = hasSummary ? messages.get(0).getContent() : "（无会话摘要）";

        StringBuilder recentConversation = new StringBuilder();
        for (int i = recentMessageStart; i < currentMessageIndex; i++) {
            Message message = messages.get(i);
            if (message == null || message.getContent() == null || message.getContent().isBlank()) {
                continue;
            }
            if (!recentConversation.isEmpty()) {
                recentConversation.append('\n');
            }
            recentConversation.append(message.getRole())
                    .append(": ")
                    .append(message.getContent());
        }

        String userPrompt = """
                <current-user-message>
                %s
                </current-user-message>
                <recent-conversation>
                %s
                </recent-conversation>
                <conversation-summary>
                %s
                </conversation-summary>
                """.formatted(
                currentMessage.getContent(),
                recentConversation.isEmpty() ? "（无最近会话消息）" : recentConversation,
                summary
        );
        //1、提取长期记忆候选
        String candidates = chatClient.prompt()
                .system(MEMORY_CANDIDATE_EXTRACTOR_SYSTEM_PROMPT)
                .user(userPrompt)
                .call()
                .content();
        if (candidates == null || candidates.isBlank()) {
            throw new IllegalStateException("大模型未返回长期记忆候选");
        }

        logger.info("长期记忆候选抽取完成，conversationId={}，candidates={}",
                currentMessage.getConversationId(), candidates.trim());
        MemoryCandidateResponse candidateResponse;
        try {
            candidateResponse = objectMapper.readValue(candidates, MemoryCandidateResponse.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("长期记忆候选不是合法 JSON", e);
        }

        Long userId = jdbcTemplate.queryForObject(
                "select user_id from `chat` where id = ?",
                Long.class,
                currentMessage.getConversationId()
        );
        if (userId == null) {
            throw new IllegalStateException("会话未关联用户: " + currentMessage.getConversationId());
        }
        //2、处理长期记忆候选
        FilterExpressionBuilder filterBuilder = new FilterExpressionBuilder();
        for (MemoryCandidate candidate : candidateResponse.candidates()) {
            if (candidate == null || candidate.memoryKey() == null
                    || candidate.memoryKey().isBlank() || candidate.content() == null) {
                logger.warn("忽略格式不完整的长期记忆候选，candidate={}", candidate);
                continue;
            }
            // language 对每个用户只保留一条，直接按用户和 memory_key 查询并更新，无需向量检索。
            if ("language".equals(candidate.memoryKey())) {
                List<Document> languageMemories = jdbcTemplate.query(
                        """
                        select id, content, version, source_message_id
                        from long_memory
                        where user_id = ? and memory_key = ? and status = ?
                        order by id desc
                        limit 1
                        """,
                        (resultSet, rowNum) -> {
                            Long memoryId = resultSet.getLong("id");
                            Integer version = resultSet.getInt("version");
                            Long sourceMessageId = resultSet.getObject("source_message_id", Long.class);
                            Map<String, Object> metadata = new HashMap<>();
                            metadata.put("longMemoryId", memoryId);
                            metadata.put("userId", userId);
                            metadata.put("memoryKey", candidate.memoryKey());
                            metadata.put("status", MEMORY_STATUS_ACTIVE);
                            metadata.put("version", version);
                            if (sourceMessageId != null) {
                                metadata.put("sourceMessageId", sourceMessageId);
                            }
                            return new Document(
                                    "long-memory-" + memoryId + "-v" + version,
                                    resultSet.getString("content"),
                                    metadata
                            );
                        },
                        userId,
                        candidate.memoryKey(),
                        MEMORY_STATUS_ACTIVE
                );
                if (languageMemories.isEmpty()) {
                    insertMemory(userId, candidate, candidate.content(), currentMessage.getId());
                } else {
                    updateMemory(
                            userId,
                            candidate,
                            candidate.content(),
                            languageMemories.get(0),
                            currentMessage.getId()
                    );
                }
                continue;
            }

            List<Document> matches = vectorStore.similaritySearch(
                    SearchRequest.builder()
                            .query(candidate.content().toString())
                            .topK(1)
                            .filterExpression(
                                    filterBuilder.and(
                                            filterBuilder.and(
                                                    filterBuilder.eq("userId", userId),
                                                    filterBuilder.eq("memoryKey", candidate.memoryKey())
                                            ),
                                            filterBuilder.eq("status", MEMORY_STATUS_ACTIVE)
                                    ).build()
                            )
                            .build()
            );
            Document topMatch = matches.isEmpty() ? null : matches.get(0);
            logger.debug("长期记忆候选向量检索完成，userId={}，memoryKey={}，top1Id={}，score={}",
                    userId,
                    candidate.memoryKey(),
                    topMatch == null ? null : topMatch.getId(),
                    topMatch == null ? null : topMatch.getScore());

            double score = topMatch == null || topMatch.getScore() == null
                    ? 0D
                    : topMatch.getScore();
            if (topMatch != null && score >= DIRECT_UPDATE_THRESHOLD) {
                updateMemory(userId, candidate, candidate.content(), topMatch, currentMessage.getId());
            } else if (topMatch != null && score >= LLM_DECISION_THRESHOLD) {
                MemoryMergeDecision decision = decideMemoryMerge(candidate, topMatch, score);
                if ("UPDATE".equalsIgnoreCase(decision.action())) {
                    updateMemory(userId, candidate, decision.mergedContent(), topMatch, currentMessage.getId());
                } else if ("INSERT".equalsIgnoreCase(decision.action())) {
                    insertMemory(userId, candidate, decision.mergedContent(), currentMessage.getId());
                } else {
                    throw new IllegalStateException("大模型返回了不支持的长期记忆操作: " + decision.action());
                }
            } else {
                insertMemory(userId, candidate, candidate.content(), currentMessage.getId());
            }
        }

    }

    private MemoryMergeDecision decideMemoryMerge(
            MemoryCandidate candidate,
            Document topMatch,
            double score
    ) {
        String prompt = """
                <memory-key>
                %s
                </memory-key>
                <existing-memory>
                %s
                </existing-memory>
                <candidate-memory>
                %s
                </candidate-memory>
                <similarity-score>
                %s
                </similarity-score>
                """.formatted(
                candidate.memoryKey(),
                topMatch.getText(),
                candidate.content(),
                score
        );
        String response = chatClient.prompt()
                .system(MEMORY_MERGE_DECIDER_SYSTEM_PROMPT)
                .user(prompt)
                .call()
                .content();
        if (response == null || response.isBlank()) {
            throw new IllegalStateException("大模型未返回长期记忆合并决策");
        }

        try {
            MemoryMergeDecision decision = objectMapper.readValue(response, MemoryMergeDecision.class);
            if (decision.action() == null || decision.action().isBlank()
                    || decision.mergedContent() == null || !decision.mergedContent().isObject()) {
                throw new IllegalStateException("长期记忆合并决策字段不完整");
            }
            return decision;
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("长期记忆合并决策不是合法 JSON", e);
        }
    }

    private void insertMemory(
            Long userId,
            MemoryCandidate candidate,
            JsonNode content,
            Long sourceMessageId
    ) {
        Long memoryId = transactionTemplate.execute(status -> {
            KeyHolder keyHolder = new GeneratedKeyHolder();
            int inserted = jdbcTemplate.update(connection -> {
                PreparedStatement statement = connection.prepareStatement(
                        """
                        insert into long_memory
                            (user_id, memory_key, content, status, version, source_message_id)
                        values (?, ?, ?, ?, ?, ?)
                        """,
                        Statement.RETURN_GENERATED_KEYS
                );
                statement.setLong(1, userId);
                statement.setString(2, candidate.memoryKey());
                statement.setString(3, content.toString());
                statement.setString(4, MEMORY_STATUS_ACTIVE);
                statement.setInt(5, 1);
                statement.setObject(6, sourceMessageId);
                return statement;
            }, keyHolder);
            Number generatedId = keyHolder.getKey();
            if (inserted != 1 || generatedId == null) {
                throw new IllegalStateException("插入长期记忆失败");
            }
            return generatedId.longValue();
        });
        if (memoryId == null) {
            throw new IllegalStateException("数据库事务未返回新增长期记忆 ID");
        }

        vectorStore.add(List.of(toActiveDocument(
                memoryId,
                userId,
                candidate.memoryKey(),
                content,
                1,
                sourceMessageId
        )));
        logger.info("新增长期记忆完成，userId={}，memoryId={}，memoryKey={}",
                userId, memoryId, candidate.memoryKey());
    }

    private void updateMemory(
            Long userId,
            MemoryCandidate candidate,
            JsonNode content,
            Document topMatch,
            Long sourceMessageId
    ) {
        Long memoryId = metadataAsLong(topMatch, "longMemoryId");
        Integer oldVersion = metadataAsInteger(topMatch, "version");
        if (memoryId == null || oldVersion == null) {
            throw new IllegalStateException("ES 长期记忆缺少 longMemoryId 或 version 元数据");
        }

        Integer newVersion = transactionTemplate.execute(status -> {
            int updated = jdbcTemplate.update(
                    """
                    update long_memory
                    set content = ?, source_message_id = ?, version = version + 1
                    where id = ? and user_id = ? and version = ? and status = ?
                    """,
                    content.toString(),
                    sourceMessageId,
                    memoryId,
                    userId,
                    oldVersion,
                    MEMORY_STATUS_ACTIVE
            );
            if (updated != 1) {
                throw new IllegalStateException(
                        "长期记忆已被并发修改或不存在，memoryId=" + memoryId + "，version=" + oldVersion
                );
            }
            return oldVersion + 1;
        });
        if (newVersion == null) {
            throw new IllegalStateException("数据库事务未返回长期记忆新版本");
        }

        Map<String, Object> deletedMetadata = new HashMap<>(topMatch.getMetadata());
        deletedMetadata.put("status", MEMORY_STATUS_DELETE);
        Document deletedDocument = new Document(
                topMatch.getId(),
                topMatch.getText(),
                deletedMetadata
        );
        Document activeDocument = toActiveDocument(
                memoryId,
                userId,
                candidate.memoryKey(),
                content,
                newVersion,
                sourceMessageId
        );
        vectorStore.add(List.of(deletedDocument));
        vectorStore.add(List.of(activeDocument));

        logger.info("更新长期记忆完成，userId={}，memoryId={}，memoryKey={}，version={}",
                userId, memoryId, candidate.memoryKey(), newVersion);
    }

    private Document toActiveDocument(
            Long memoryId,
            Long userId,
            String memoryKey,
            JsonNode content,
            Integer version,
            Long sourceMessageId
    ) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("longMemoryId", memoryId);
        metadata.put("userId", userId);
        metadata.put("memoryKey", memoryKey);
        metadata.put("status", MEMORY_STATUS_ACTIVE);
        metadata.put("version", version);
        if (sourceMessageId != null) {
            metadata.put("sourceMessageId", sourceMessageId);
        }
        return new Document(
                "long-memory-" + memoryId + "-v" + version,
                content.toString(),
                metadata
        );
    }

    private Long metadataAsLong(Document document, String key) {
        Object value = document.getMetadata().get(key);
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value == null) {
            return null;
        }
        try {
            return Long.valueOf(value.toString());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private Integer metadataAsInteger(Document document, String key) {
        Long value = metadataAsLong(document, key);
        return value == null ? null : value.intValue();
    }

    private record MemoryCandidateResponse(List<MemoryCandidate> candidates) {
        private MemoryCandidateResponse {
            candidates = candidates == null ? List.of() : candidates;
        }
    }

    private record MemoryCandidate(
            @JsonProperty("memory_key") String memoryKey,
            JsonNode content
    ) {
    }

    private record MemoryMergeDecision(
            String action,
            @JsonProperty("merged_content") JsonNode mergedContent
    ) {
    }

    private boolean isSummary(Message message) {
        return message != null
                && "system".equalsIgnoreCase(message.getRole())
                && message.getContent() != null
                && message.getContent().startsWith(SUMMARY_PREFIX);
    }
}
