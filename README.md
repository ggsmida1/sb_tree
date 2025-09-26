SB-Tree（单线程、只追加阶段）

目标：不考虑延迟/乱序数据，先完成顺序插入与查询（点查），并构建一个批量晋升、只追加的 B+ 风格搜索层。

架构概览

数据层（DataBlock 链）
顺序插入的数据先聚集在段中，触发转换后切成一串 DataBlock 按顺序尾插到全局链表。每个块内用 N-ary 表 辅助查找。

搜索层（SearchLayer，vector 版）

叶层 L0：为每个 DataBlock 记录 {min_key, ptr}。

内层 L1/L2/...：当某层“新增条目”凑满扇出 F 时，批量晋升为父条目，父条目覆盖下层连续 F 个孩子。

未凑满 F 的“尾巴”保留到下次晋升（只追加，不分裂/合并）。

查询

find_candidate(k)：自顶向下在已覆盖区间做 floor，返回“最后一个 min_key ≤ k 的叶块”。

lookup(k)：在候选块内查找；若未命中则沿 next() 右移兜底直到 next->min_key() > k。

已完成

✅ 顺序插入（append-only），段转换 → DataBlock 链尾插。

✅ 搜索层自下而上批量晋升，扇出 F（默认 64，可配置）。

✅ 点查（lookup）：索引定位 + 块内 N-ary 查找 + next 兜底。

✅ 基础测试（gtest + 手写测试）：

凑满 F 才晋升（promotion）

端到端查找（lookup）

尾巴未晋升（candidate & lookup 正确）

你原有的插入/验证用例均通过

参数与不变量

扇出 F：默认 64（构造 SearchLayer 时可设）。所有层使用相同 F。

不变量（关键校验）

L0 的 min_key 非降；

每个父条目 child_begin..child_begin+child_count-1 在下层范围内，且 min_key == 第一个孩子的 min_key；

“推进指针”（已晋升到的位置）不越界；

只有当新增条目数凑满 F 才晋升；不足 F 的尾部保留到下次。

测试

gtest 用例

SearchLayerPromotion.OnlyPromoteWhenFullF：验证“凑满 F 才晋升”

SBTreeLookup.EndToEndOrderedKeys：端到端查找

SearchLayerTailUnpromoted.CandidateWorks：尾巴未晋升

你原有的测试

test_insert / test_single_insert：顺序插入 + 验证

手写示例（test_manual.cpp）演示插入 & 查找。

已知限制

仅支持单线程路径（还未加并发/锁分层/专线索引线程）。

暂不处理延迟/乱序数据（即“延迟数据”未插入到已有块中）。

搜索层使用内存中的 vector 结构（SearchBlock 抽象与持久化尚未实现）。

没有块合并/分裂（与“只追加、批量晋升”的策略一致）。

没有持久化/恢复逻辑（仅内存型）。

接下来要做

优先级 A（功能闭环）

Range Scan：scan(l, r, callback)（索引定位起点块 → 跨块扫描 → 早停）。

延迟/乱序数据的暂存与合并策略（如 per-thread buffer / delta 区 + 后台归并）。

SearchBlock 抽象：把 vector 版替换为块封装，统一元数据管理（为后续并发与持久化做准备）。

优先级 B（并发 & 稳定性） 4. 索引构建专线线程：append_run 由单线程队列驱动；读路径只读索引 + next 兜底。 5. 锁分层/读优化：数据层读尽量无锁或细粒度锁，写在转换/接链处加短临界区。 6. 内存管理：明确 DataBlock 的所有权与生命周期（search 不持有，压缩/淘汰策略）。

优先级 C（优化 & 工程化） 7. find_candidate 尾部向右窥视一步的小优化（可选，功能无依赖）。 8. 参数调优（F、块大小、N-ary 表阶数）；基准脚本。 9. 持久化/恢复（如快照+WAL），监控与统计（层大小、晋升次数、QPS、延迟等）。
