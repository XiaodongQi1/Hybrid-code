#!/usr/bin/env python3
"""Summarize sealed storage groups and separately verified Snappy projections."""
import csv
import json
import sys
from pathlib import Path

root = Path(sys.argv[1]).resolve()
groups = json.loads((root / 'results.json').read_text())
completion = json.loads((root / 'completion.json').read_text())
projections = json.loads((root / 'snappy-projection/results.json').read_text())
projection_done = json.loads((root / 'snappy-projection/completion.json').read_text())
if not completion['Verified'] or not projection_done['Verified'] or not projection_done['SourceUnchanged']:
    raise SystemExit('unverified inputs')
if len(groups) != completion['Groups'] or len(projections) != len(groups):
    raise SystemExit('group count differs')
lookup = {(p['Workload'], p['Method']): p for p in projections}
rows = []
for group in groups:
    if not group['Verified'] or not group['ReadOnlyUnchanged']:
        raise SystemExit('unverified group')
    p = lookup[(group['Workload'], group['Method'])]
    if p['TraceID'] != group['TraceID'] or p['ExperimentBinaryID'] != group['BinaryID']:
        raise SystemExit('projection identity differs')
    if not p['Initial']['Verified'] or not p['Final']['Verified']:
        raise SystemExit('unverified projection')
    a, b = group['Initial'], group['Final']
    cf = lambda m, name: m['Families'][name]['KeyBytes'] + m['Families'][name]['ValueBytes']
    row = dict(Workload=group['Workload'], Method=group['Method'],
               InitialKVBytes=a['LogicalBytes'], IncrementalKVBytes=b['LogicalBytes']-a['LogicalBytes'],
               PrefixBytes=cf(b, 'prefix')-cf(a, 'prefix'), HashBytes=cf(b, 'hash')-cf(a, 'hash'),
               MetadataBytes=cf(b, 'metadata')-cf(a, 'metadata'),
               ActualUncompressedSSTBytes=b['LiveSSTBytes']-a['LiveSSTBytes'],
               InitialNormalizedSnappySSTBytes=p['Initial']['SSTBytes'],
               NormalizedSnappySSTBytes=p['Final']['SSTBytes']-p['Initial']['SSTBytes'],
               FinalHashCopyBytes=b['HashCopyBytes'],
               HashProbes=group['ExecutionCommits']['ClosureReads'],
               HashValueReadBytes=group['ExecutionCommits']['ClosureReadBytes'],
               UnchangedBackfillBytes=group['ExecutionCommits']['ClosureBackfillBytes'],
               PeakBackfillHeight=group['MaxBackfillHeight'], PeakBackfillBytes=group['MaxBackfillBytes'],
               RootChecks=group['RootChecks'], VerifiedProofs=group['VerifiedProofs'],
               TraceID=group['TraceID'], BinaryID=group['BinaryID'])
    if row['PrefixBytes'] + row['HashBytes'] + row['MetadataBytes'] != row['IncrementalKVBytes']:
        raise SystemExit('KV accounting mismatch')
    for measurement in (a, b):
        if measurement['LiveSSTBytes'] != measurement['SSTDirectoryBytes']:
            raise SystemExit('live and on-disk SST disagree after close')
    rows.append(row)
baselines = {r['Workload']: r for r in rows if r['Method'] == 'native-hash-archive'}
for r in rows:
    baseline = baselines[r['Workload']]
    r['KVSavingPercent'] = 100 * (1-r['IncrementalKVBytes']/baseline['IncrementalKVBytes'])
    r['NormalizedSnappySavingPercent'] = 100 * (1-r['NormalizedSnappySSTBytes']/baseline['NormalizedSnappySSTBytes'])
(root / 'analysis.json').write_text(json.dumps(rows, ensure_ascii=False, indent=2)+'\n')
with (root / 'analysis.csv').open('w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)

config = groups[0]['Config']
mib = lambda n: n / 2**20
parts = ['# HybridTrie 第三步实验结果', '',
         f"已完成{len(groups)}组固定布局实验：{config['Keys']}个键、初始化后{config['Blocks']}个块、每块{config['WritesPerBlock']}次写入、{config['ValueBytes']}字节值、seed={config['Seed']}。每种负载7种方法；热点负载将90%的选择限制在固定20%键中。所有数字均排除初始化，采用各方法自身期末减实际起点。", '',
         '这是合成单Trie的物理表示实验，使用同一个原生MPT逻辑实现；未执行EVM，也未实现或计量实际N/H缓存。固定连通边界仅模拟引用选择。全Prefix代表全内存引用极限，不能直接当成有界缓存方案的收益。', '',
         '## 存储结果', '',
         '|负载|方法|初始化KV MiB|执行KV MiB|执行无压缩RocksDB SST MiB|执行统一Snappy投影 MiB|KV较Archive减少|投影较Archive减少|',
         '|---|---|---:|---:|---:|---:|---:|---:|']
for r in rows:
    parts.append(f"|{r['Workload']}|{r['Method']}|{mib(r['InitialKVBytes']):.3f}|{mib(r['IncrementalKVBytes']):.3f}|{mib(r['ActualUncompressedSSTBytes']):.3f}|{mib(r['NormalizedSnappySSTBytes']):.3f}|{r['KVSavingPercent']:.2f}%|{r['NormalizedSnappySavingPercent']:.2f}%|")
parts += ['', 'KV包含全部Key、Value、Prefix、Hash、根目录及其他应用元数据。RocksDB 9.10.0使用无压缩设置，两端均同步flush并全范围compact后测live SST；与关闭后的实际SST文件字节一致。目录/WAL/其他文件另存于原始JSON。', '',
          '统一Snappy投影逐CF保留完整live KV，以GoLevelDB table格式、Snappy、4KiB块、restart=16生成；它不是RocksDB启用Snappy后的原生空间。每个生成的SST均重开，逐条验证有序KV摘要。初始化只重建snapshot且核对原始inventory，终点直接读取封存数据库；投影前后原库指纹一致。两个端点之差也不等同于单独隔离出的history文件体积。', '',
          '## 解释', '',
          '- 两层固定边界的节省率：' + '；'.join(f"{r['Workload']}的KV {r['KVSavingPercent']:.2f}%，Snappy投影 {r['NormalizedSnappySavingPercent']:.2f}%" for r in rows if r['Method']=='frontier-2') + '。混合表示的收益需结合边界解释。',
          '- 仅根下保留全部H摘要的节省率：' + '；'.join(f"{r['Workload']}的KV {r['KVSavingPercent']:.2f}%，Snappy投影 {r['NormalizedSnappySavingPercent']:.2f}%" for r in rows if r['Method']=='root-all-H') + '。负节省率表示增加，逻辑字节与压缩体积必须分别报告。',
          '- full-hash-dual额外保存Prefix和Hash，不是原生Archive。它较Archive更大，说明当前保守闭包副本是重要开销，不能把它当成唯一基准以夸大收益。',
          '- 全Prefix节省最多，但依赖所有需要的孩子摘要可用；完整双缓存必须通过预算、命中率及执行代价共同评估。', '',
          '## 边界切换与回填', '',
          f"frontier-2-switch在高度{config['Blocks']//2+1}至{config['Blocks']*3//4}切到全Hash，再恢复两层边界。以下计数排除初始化；回填指新增Hash记录中不在本块新Prefix digest集合里的部分。", '',
          '|负载|两层固定KV MiB|切换组KV MiB|切换组Hash增量 MiB|未变内容回填 KiB|回填峰值高度|峰值 KiB|',
          '|---|---:|---:|---:|---:|---:|---:|']
for workload in baselines:
    fixed = next(r for r in rows if r['Workload']==workload and r['Method']=='frontier-2')
    switch = next(r for r in rows if r['Workload']==workload and r['Method']=='frontier-2-switch')
    parts.append(f"|{workload}|{mib(fixed['IncrementalKVBytes']):.3f}|{mib(switch['IncrementalKVBytes']):.3f}|{mib(switch['HashBytes']):.3f}|{switch['UnchangedBackfillBytes']/1024:.2f}|{switch['PeakBackfillHeight']}|{switch['PeakBackfillBytes']/1024:.2f}|")
parts += ['', '回填峰值见表；切换组额外空间还来自后续持续保存显式引用及Hash副本，不能把全部差异都归因于一次性回填。静态边界组在本轮只有值更新、没有插删且拓扑不变的负载下没有未变内容回填，不能据此声称任意负载的转换无成本。Hash探测次数、返回Value字节和逐块写入数据见blocks.csv；它们不是物理磁盘I/O。commit耗时只用于诊断，没有重复性能轮，不代表EVM在线执行性能。', '',
          '## 校验与产物', '',
          f"完成{completion['Groups']}组，根检查{completion['RootChecks']:,}次，历史证明{completion['Queries']}条。每组Verified和ReadOnlyUnchanged均为true；投影覆盖全部CF数据并逐条验证。", '',
          '- results.json / results.csv：原始两端测量与汇总。',
          '- 各负载／方法目录：db、blocks.csv、result.json完成检查点。',
          '- snappy-projection：压缩投影SST、每CF摘要和验证结果。',
          '- analysis.json / analysis.csv：供后续绘图的完整统计表。',
          '- environment.json / binary-provenance.json：原实验身份和已保留可执行文件的指纹。', '',
          '下一步可实现有界N/H双缓存并测量真实内存与执行开销。当前结果支持混合表示的可行性，同时要求继续关注浅边界及Hash闭包副本；不据本轮结果宣称所有缓存配置都优于Archive。', '']
(root / 'analysis.zh.md').write_text('\n'.join(parts))
print(root / 'analysis.zh.md')
for r in rows:
    if r['Method'] in ('native-hash-archive', 'full-prefix', 'frontier-2', 'root-all-H'):
        print(r['Workload'], r['Method'], f"KV={mib(r['IncrementalKVBytes']):.3f} MiB", f"Snappy={mib(r['NormalizedSnappySSTBytes']):.3f} MiB", f"KV saving={r['KVSavingPercent']:.2f}%", f"Snappy saving={r['NormalizedSnappySavingPercent']:.2f}%")
