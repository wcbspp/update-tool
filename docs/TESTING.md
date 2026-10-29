# 测试

三层：**单元**（纯函数，无外部依赖）→ **接口集成**（打 `handler`，用临时目录，不连库）→ **端到端冒烟**（真连测试库，走预检→执行→核对结果）。

## 运行

```bash
# 只跑单元（无外部依赖，约 1 秒）
go test ./...

# 全量：带上测试库才能跑数据库相关用例（会真建表、真删表）
MYSQL_TEST_DSN='root:口令@tcp(127.0.0.1:3306)/medupdate_test' go test ./...

# 只看某一层
go test -run 'TestAPI'                          # 接口
MYSQL_TEST_DSN=... go test -run 'TestSmoke' -v   # 端到端
```

**安全闸**：`smoke_test.go` 的 `TestMain` 会检查 `MYSQL_TEST_DSN`，DSN 里没有 `test` 字样直接拒绝运行，防止误连生产库。

**不能并发跑**：测试用固定表名（`smoke_t`、`tx_probe`、`zz_*`），彼此 DROP，必须串行。Go 同包默认串行，不要去加 `t.Parallel()`。

## 覆盖矩阵

按设计模块对照，`✅` = 有对应用例。

| 设计能力 | 覆盖 | 主要用例 |
|---|---|---|
| SQL 拆分（引号、注释、`\` 转义、分隔符） | ✅ | `TestSplitStatements*`、`TestSplitStatementsComments`、`TestSplitStatementsBackslashEscape`、`TestTonightEdgeCases` |
| 语句类型识别（含 REPLACE、INSERT…SELECT） | ✅ | `TestStmtKind`、`TestStmtKindReplace`、`TestDeleteModifiers` |
| 拒绝 DDL / 脚本内 COMMIT-ROLLBACK | ✅ | `TestExecGuardRejectsImplicitCommit`、`TestSmokeDDLBlocked`、`TestSmokeExplicitCommitBlocked` |
| 无 WHERE 的 UPDATE/DELETE 执行入口拦截 | ✅ | `TestFindUnsafeStatement`、`TestUnsafeMultiTableDelete`、`TestSmokeUnsafeUpdateBlocked`、`TestSmokeUnsafeDeleteBlocked` |
| 单事务、任一条失败整体回滚 | ✅ | `TestRunStatementsRollback`、`TestSmokeRollbackOnFailure` |
| 演练模式不改数据 | ✅ | `TestRunStatementsDryRun`、`TestExecDryRunLeavesNothing`、`TestSmokeDryRun` |
| 非事务引擎写入拦截 | ✅ | `TestSmokeNonTransactionalEngine` |
| 只读保护（配置损坏拒写） | ✅ | `TestCorruptCiphertextRefusesWrite`、`TestLoadRejectsUnknownFormat`、`TestLoadEmptyFileIsCorrupt` |
| 预检：主键冲突 / 列数 / 列存在 / 行数 | ✅ | `TestPreflightInsertConflictDedup`、`TestPreflightInsertReorderedColumns`、`TestPreflightBogusSetColumn`、`TestPreflightDeleteExactCount` |
| 预检：版本号核对 | ✅ | `TestPreflightVersionWithoutPrefixes`、`TestPreflightNoVersionColumnSkipsCheck` |
| 预检：同表多语句条件重叠 | ✅ | `TestCondsOverlap*`、`TestOverlapVerdict`、`TestPreflightMultiStatementSameTable` |
| 预检：子查询 / 别名 / 复杂源不猜 | ✅ | `TestPreflightUpdateSubqueryNoWhere`、`TestPreflightDeleteWithSubquery`、`TestPreflightUpdateSetWithoutAlias`、`TestPreflightInsertSelect` |
| 元数据兜底（中间件屏蔽 information_schema） | ✅ | `TestMetadataFallbackWhenInformationSchemaBlocked`、`TestMetaCacheHits` |
| 超时（连接/DQL/DML/预检） | ✅ | `TestExecTimeoutIsPerStatement`、`TestDsPFSec`、`TestDMLSecUnlimited` |
| 配置加密、明文迁移、密文损坏 | ✅ | `TestSealOpenCfg`、`TestOpenCfgTampered`、`TestLoadLegacyPlaintext`、`TestSaveMigratesToCiphertext`、`TestSaveNonceVaries` |
| 导出 / 导入（范围、口令、ID 重建、幂等） | ✅ | `TestBuildExportScopes`、`TestPlanImport*`、`TestImportDSConflictIdempotent`、`TestAPIExportImport` |
| 接口：数据源/文件夹/脚本 CRUD | ✅ | `TestAPIDatasourceCRUD`、`TestAPIScriptCRUD` |
| 接口：口令不回传前端 | ✅ | `TestAPIDatasourceCRUD`（检查 `/api/tree` 不含口令） |
| 接口：监听范围切换与重启语义 | ✅ | `TestAPISettings`、`TestAPISettingsListenModeSwitch` |
| 接口：忙碌锁挡住配置写入 | ✅ | `TestAPIBusyBlocksConfigWrites`、`TestNotBusyPassesThrough` |
| 接口：`/dl/` 白名单、路径穿越、符号链接 | ✅ | `TestAPIReleaseDownloadWhitelist` |
| 接口：日志写读清空 | ✅ | `TestAPILogsEmpty`、`TestAPILogsClear` |
| 拦截：跨 MySQL 8.4 / MariaDB 11.6 语法差异 | ✅ | `TestSQLCompatMatrix`、`TestMariaDBVersion` |
| 性能：大脚本 / 大表 | ✅ | `TestPerfScenarios`、`TestPerfExecute` |

## 按钮 → 用例对照

每个可点功能都必须有用例，且用例要断言**具体字段/具体文案**，不能只看状态码。

| 界面功能 | 接口 | 用例 |
|---|---|---|
| 数据预检 | `POST /api/preflight` | 15 个 `TestPreflight*` + `TestPreflightWritesNothing` + `TestPreflightWithReadOnlyAccount` |
| 执行 | `POST /api/run` | `TestSmoke*`（8 个）、`TestExec*`（8 个） |
| 语法校验 | `POST /api/check` | `TestAPICheck` |
| 保存脚本 | `POST /api/script` | `TestAPIScriptCRUD` |
| 设置（含监听范围） | `GET/POST /api/settings` | `TestAPISettings`、`TestAPISettingsListenModeSwitch` |
| 执行日志 / 清空 | `GET/DELETE /api/logs` | `TestAPILogsEmpty`、`TestAPILogsClear` |
| 使用说明 | `GET /guide` | `TestAPIPages` |
| 版本 / 检查更新 | `GET /api/update/check` | `TestAPIUpdateCheckShape`、`TestEffectiveUpdateURL` |
| 导出 / 导入 | `POST /api/export`、`/api/import` | `TestAPIExportImport`、`TestPlanImport*`（9 个）、`TestBuildExportScopes` |
| 新建 / 编辑数据源 | `POST /api/ds` | `TestAPIDatasourceCRUD`、`TestAPIDatasourcePortDefault` |
| 测试连接 | `POST /api/ds/test` | `TestAPIDatasourceTest` |
| 删除数据源 | `DELETE /api/ds` | `TestAPIDeleteDatasourceWithChildren` |
| 新建文件夹 / 子文件夹 | `POST /api/group` | `createScript` 前置、`TestAPIDeleteGroupCascade` |
| 删除文件夹（级联） | `DELETE /api/group` | `TestAPIDeleteGroupCascade` |
| 新建 / 删除脚本 | `POST/DELETE /api/script` | `TestAPIScriptCRUD` |
| 上移 / 下移 | `POST /api/order` | `TestAPIOrderMove` |
| 退出程序 | `POST /api/shutdown` | `TestAPIShutdownLoopbackOnly` |
| 下载安装包 | `GET /dl/…` | `TestAPIReleaseDownloadWhitelist` |
| 诊断接口（默认关闭） | `GET /api/inspect` | `TestAPIInspectClosedByDefault` |
| 图标 / favicon | `GET /icon*`、`/favicon.ico` | `TestAPIIcons` |
| 编辑 / 美化 / 还原 / 复制结果 / 折叠 / 搜索 | 纯前端 | 无接口，靠手工验收 |

## 预检「零写入」怎么证明

不靠读代码判断，靠两条可复现的证据：

1. **前后比对**（`TestPreflightWritesNothing`）：预检一个含 `UPDATE`/`INSERT`/`DELETE` 的脚本，把相关表整份 dump（按主键排序、逐行逐列序列化），跑完再 dump 一次，要求一字不差。
2. **只读账号**（`TestPreflightWithReadOnlyAccount`）：临时建一个只授予 `SELECT` 的 MySQL 账号，用它跑完整预检。用例会先反向确认这个账号确实写不了（写一次必须报权限错误），再要求预检能正常跑完、且不出现任何 `error`。真偷写一句，这里就直接失败。

## 执行「完整性」怎么证明

1. **每条都生效**（`TestExecAppliesEveryStatement`）：五条语句跨两张表，逐条查库确认，并核对返回的受影响行数合计。
2. **任意位置失败都零残留**（`TestExecFailureAtAnyPositionLeavesNothing`）：把必然失败的语句依次插到第 1～5 位，每次都要求两张表 dump 与执行前完全一致、失败位置准确。
3. **驱动没有 multiStatements**（`TestDriverRejectsMultiStatements`）：一条 `Exec` 里塞两条语句必须报错，否则事务边界不可靠。
4. **演练不改数据**（`TestExecDryRunTouchesNothing`）：四条语句全跑一遍，库里零变化。

## 没覆盖的（已知缺口）

- **单实例接管**（`/api/shutdown` 让旧进程退出后新进程接管）：需要起两个进程，没做自动化。
- **更新检查**（拉 `version.json`）：依赖外网，没做。
- **监听范围真实重绑**（`restartSelf`）：只测到「返回 needRestart」，没测真重启。
- **`ACCESS_KEY` 鉴权**：中间件在 `main()` 里装配，`handler` 单测绕过了它。
- **超长执行 / 30 万行上限**：只测了逻辑分支，没跑真到上限。
- **跨库写入的次要检查**：`INSERT INTO 其他库.t` 的引擎检查已按目标库查，但列数、主键、VALUES 解析这几项仍按连接库的 schema 走，跨库时会静默落空。属既有问题，本工具的实际用法都在单库内，暂未处理。

## 结果（2026-10-30）

| 环境 | 结果 | 耗时 |
|---|---|---|
| 无测试库（纯单元） | 80 通过 / 34 跳过 / 0 失败 | ~1.1s |
| MySQL 8.4.13 `127.0.0.1:3306/medupdate_test` | 110 通过 / 3 跳过 / 0 失败 | 2.8s |
| MariaDB 11.6.2 `127.0.0.3:9122/medupdate_test` | 110 通过 / 3 跳过 / 0 失败 | 6.6s |

共 114 个用例函数。跳过的 3 个是与当前服务端能力相关的分支（例如当前身份能写目录时无法构造保存失败场景）。

## 本轮测出来的缺陷（已修）

**生产代码**

1. **非事务引擎只查批次表**：`engineWarnings` 之前只拿 `*_update_2107` 批次表，往普通 MyISAM 目标表写数据却报 ok。已改为按 INSERT/UPDATE/DELETE 的真实目标表检查。
2. **跨库写入查错引擎**：`INSERT INTO 其他库.t` 经 `normTable` 砍掉库名，按当前库去查，跨库 MyISAM 漏报、同名 InnoDB 误报。已保留库名前缀按它自己的 schema 查。
3. **UPDATE 目标表定位不到时整表缺席**：SET 没写别名且列名无法归属时，`writeTables` 里没有这张表，可能写 MyISAM 而静默通过。现在把参与的表按「可能被写」检查，给 warn 提醒。

**测试代码自身（评审发现，避免假绿）**

4. `/api/logs` 断言用了不存在的 `total` 字段，两条校验永不执行——写日志和清空日志实际没被验证。
5. 符号链接建不起来时只打日志，404 断言照跑，软链防护等于没测。
6. DDL 只断言 `code != 200`，404/500 也算通过，且没确认列没被加上。
7. MyISAM 用例只要出现任意 error 就算通过，且只测 INSERT。
8. 防误连生产库的闸门只看整串 DSN 里有没有 `test`，口令或主机名带 test 就放行——已改为解析 DSN 只认库名 `_test` 结尾。
9. 注释写「保存失败」但只跑成功路径——已补目录只读的失败用例。

## 约定

- 新增用例必须**能真失败**：断言要检查具体字段和具体文案，不能只看状态码。
- 跳过（`t.Skip`）必须说明原因，且不能掩盖该测的行为。
- 涉及 DROP/写入的用例一律走 `MYSQL_TEST_DSN`，禁止指向生产库。
