# CF001 代工机型激活体系 · 实现说明与差异记录

服务：`cmd/cf001-svc`（:8087），代码 `internal/cf001`，表 `sql/cf001.sql`（schema `cf001`）。

## 1. 密码学链路

```
设备                                          云端 (cf001-svc)
────                                          ────────────────
uuid = 随机生成（设备内，不外泄）
digest = hex(SHA256(uuid ":" mcuSN ":" socSN ":" mac))
plaintext = digest "|" nonce "|" timestamp     ──OAEP(SHA-256, 云公钥)──▶  私钥解密 → ParsePayload
                                                                          ├─ digest 已存在 → 返回既有 SN + 重签（幂等，不耗配额）
                                                                          └─ 配额事务 → SN → PSS 签 digest → 落库
                                              ◀── {sn, signature_b64} ──
设备自检：公钥 PSS 验签 signature 对 digest       （或调 /verify 让云端三步自检）
```

| 环节 | 算法 | 实现 |
|---|---|---|
| 身份摘要 | `Digest()` = SHA-256，四段用 `:` 拼接 | `crypto.go`，固定向量单测 |
| 认证载荷 | RSA-OAEP，MGF1/SHA-256，无 label | `EncryptOAEP` / `DecryptOAEP` |
| 自检签名 | RSA-PSS，SHA-256，salt = hash 长度（32） | `SignDigestPSS` / `VerifyDigestPSS`，签名对象是 digest 的 **hex 字符串字节** |
| 密钥 | `IOT_CF001_KEY`（默认 `./data/keys/dev.pem`，PKCS#1 或 PKCS#8） | 缺失时生成内存 2048 位密钥并 `WARN`（重启后旧签名失效，仅开发） |
| 公钥分发 | `GET /api/v1/oem/pubkey` 返回 PKIX PEM | 设备/测试用它加密与验签 |

**为什么 uuid 要进 digest**：代工厂掌握全部 mcuSN/socSN/mac 清单。如果 digest 只由这三者构成，供应商可以离线批量算出 digest、
伪造激活请求提前把配额吃掉（或把 SN 卖给灰色渠道）。uuid 在设备内随机生成、只以 OAEP 密文出厂，供应商无法反推——
digest 就从「硬件指纹」变成「硬件指纹 + 一次性秘密」。

## 2. SN 规则（23 位）

```
{pk4}{yymmdd}{line2}{seq9}{ck2}
 LM_S 240918   01   000000001 95
```
- `pk4`：product_key 前 4 字符大写，不足右补 `_`（`Prefix4`）
- `yymmdd`：服务端 **UTC** 日期（多副本流水号用同一天；如需产线本地日期改 `Service.Now`）
- `line2`：请求 `line` 字段，默认 `01`（`NormalizeLine`）
- `seq9`：Redis `INCR sn:seq:{yymmdd}`，首次 `EXPIRE 2 天`；9 位零填充（`%09d`，超 10^9 取模）
- `ck2`：`Checksum(前 21 位)` = 全部字节 ASCII 求和 `% 256` → 两位大写 hex

纯函数 `BuildSN` / `Checksum` / `ValidSN` 全部表驱动单测（`sn_test.go`）。

## 3. 配额事务

```sql
BEGIN;
UPDATE cf001.oem_quotas
   SET registered = registered + 1,
       status     = CASE WHEN registered + 1 >= quota THEN 2 ELSE status END
 WHERE order_no = $1 AND status = 1 AND registered < quota
 RETURNING product_key;          -- 0 行 → ROLLBACK → HTTP 409 code 11010
INSERT INTO cf001.digest_maps(...);   -- digest 主键；23505 → 回滚，改为 SELECT 既有 SN 返回
INSERT INTO cf001.oem_devices(...);
COMMIT;
```
- 并发正确性完全由 **单条 UPDATE 的行锁 + WHERE 条件** 保证：100 个 goroutine 抢 10 个配额，恰好 10 成功、90 得到 11010
  （`service_it_test.go::TestIntegration_QuotaRace`，`IOT_IT=1 go test -race`）。
- `ck_quota_not_exceeded CHECK (registered <= quota)` 是第二道护栏：即使代码写错，库也不允许超发。
- `POST /quotas/{order_no}/append {delta}` 做 `quota = quota + delta, status = 1`：已完成工单追加配额后自动恢复可用。

## 4. 幂等与竞态

| 情形 | 行为 |
|---|---|
| 同一 digest 再次 `/sign` | 先查 `digest_maps`，命中则返回既有 SN + **重新 PSS 签名**（PSS 随机盐，签名字节不同但都有效），`existing: true`，**不消耗配额** |
| 同一 digest 两个请求同时进入事务 | 一个先 COMMIT；另一个 INSERT `digest_maps` 撞主键（23505）→ 回滚（配额 +1 一并撤销）→ 查既有返回 |
| 设备验证 | `POST /verify {sn, digest, signature_b64}` 三步：`exists` → `digest` → `signature`，失败返回 400 code 10001 + 失败的 `step` |

## 5. 限流

| 接口 | 维度 | 参数 | 原因 |
|---|---|---|---|
| `POST /api/v1/oem/sign` | **IP** | 10 次/分钟（rps 10/60，burst 10），第 11 次 429 code 100012 | 设备此时还没有身份，IP 是唯一维度；接受 NAT 误伤（见 incident 文档） |
| `POST /api/v1/oem/verify` | **deviceId + path**，在 `Security` 中间件之后 | rps 1 burst 2，同设备 1 秒内第 3 次 429 | 事故复盘的结论；数值故意小以便演示 |

`httpx.RateLimitByKey` 是进程内令牌桶，多副本下上限会被放大 N 倍（已知、已在方案标注）。

## 6. 开发 vs 生产差异表

| 项 | 开发（本原型） | 生产要求 |
|---|---|---|
| 密钥对 | **一对** RSA-2048 同时做 OAEP 解密和 PSS 签名 | **两对**：加密密钥对 + 签名密钥对；签名私钥进 **HSM/KMS**，服务只拿签名句柄 |
| 密钥来源 | `make keys` 生成到 `./data/keys/`（gitignore）；缺失时内存临时密钥 | 由 KMS 托管，禁止落盘；轮换有版本号，公钥带 kid 下发 |
| 数据库凭据 | DSN 明文环境变量 `IOT_PG_DSN` | Secret 管理 + 短期凭据 |
| 服务间调用 | 无鉴权 / 静态 token | mTLS 或签名的服务 token |
| 时间戳/nonce | 解析但**不校验新鲜度、不防重放** | 校验 `|now - timestamp| < 5min`，nonce 入 Redis 去重 |
| 限流 | 进程内令牌桶 | Redis 集中令牌桶（或网关层） |
| 传输 | HTTP 明文 | TLS（设备侧 pin 云证书） |
| SN 日期 | 服务端 UTC | 产线本地时区，需统一约定 |

## 7. 差异记录（需求文档 vs 实现）

| # | 需求文档原文/示例 | 问题 | 实现处理 |
|---|---|---|---|
| 1 | SN 校验码示例结果 **`93`** | 按文档自己定义的算法（前 21 位 ASCII 求和 % 256 → 两位大写 hex），示例应得 **`77`**（如 `XTCF24010101000000009` 求和 1143，1143 % 256 = 119 = 0x77）。`93` 是手算笔误 | **以算法为准**；`sn_test.go::TestChecksum` 固定该样例断言 `"77"`，并在注释里说明文档值有误。若后续确认文档另有算法（例如求和不含产线位），改一个纯函数即可 |
| 2 | 认证载荷示例出现第 4 段 `voltage`（`digest\|nonce\|timestamp\|voltage`），但同一文档用「三段共 103 字节」计算 OAEP 明文长度 | 第 4 段与长度计算自相矛盾；且电压不属于认证信息 | `ParsePayload` **只取前三段，第 4 段及以后忽略**（兼容两种固件），少于三段报错；单测覆盖 3 段 / 4 段 / 2 段。RSA-2048 OAEP/SHA-256 明文上限 190 字节，三段 103 字节和带短电压的四段都放得下 |

## 8. 运行

```bash
make keys                      # 生成 data/keys/dev.pem（可选；缺失则内存密钥 + 警告）
make run-cf001-svc             # :8087
curl -s localhost:8087/api/v1/oem/pubkey
curl -s -XPOST localhost:8087/api/v1/oem/quotas -d '{"order_no":"PO-1","supplier":"ACME","product_key":"LM_S1","quota":10}'
# 用 pubkey 对 "digest|nonce|ts" 做 OAEP 加密并 base64 → payload_b64
curl -s -XPOST localhost:8087/api/v1/oem/sign -d '{"order_no":"PO-1","line":"01","payload_b64":"..."}'
IOT_IT=1 go test -race -count=1 ./internal/cf001/   # 配额竞态 + DDL 约束证明
```
