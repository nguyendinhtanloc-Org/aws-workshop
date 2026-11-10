# KẾ HOẠCH VẬN HÀNH DỰ ÁN 3 TUẦN CHO NHÓM 5 NGƯỜI (INDEPENDENT TEAM TIMELINE)
**Dự án:** Nền tảng GHN Enterprise Data Lakehouse Platform  
**Mô hình vận hành:** 5 Luồng Độc Lập Hoàn Toàn (Contract-Driven, Loose Coupling)  
**Thời gian thực tế:** 10/11/2026 – 30/11/2026 (3 Tuần làm việc, 15 Ngày công chính thức)  
**Nguyên tắc độc lập:** Mỗi thành viên sở hữu 1 module khép kín, có dữ liệu mẫu (Mock data/Contracts) và test runner riêng, không ai phải chờ ai để làm việc!

---

## 📌 BẢNG ĐIỀU KHIỂN TIẾN ĐỘ THỰC TẾ (PROJECT STATUS AS OF 10/11/2026)

> [!IMPORTANT]
> **Hiện trạng tổng thể:** Dự án đã hoàn thành toàn bộ **Kiến trúc v5.0 (Proposal RFC)**, 3 tài liệu ADR chuẩn mực, bộ SQL DDL/DML Athena Serverless Iceberg và Two-Level State Machine. Toàn bộ test suite **69/69 passed xanh 100%**.  
> Dưới đây là vị trí chính xác của từng người tại thời điểm bắt đầu (10/11/2026), phần việc **đã có sẵn mã nguồn** và **nhiệm vụ cần bắt tay vào làm ngay**:

| Thành viên | Trạng thái hiện tại | Ngày công tương đương | Mã nguồn & Tài liệu đã có sẵn trong Repo | Việc cần làm tiếp theo ngay hôm nay |
| :--- | :--- | :--- | :--- | :--- |
| **👤 Member 1**<br>*(Tech Lead & Webhook)* | 🟢 **Đang ở D6**<br>(Xong 100% Tuần 1) | **D1 → D5: ĐÃ XONG**<br>D6: Đang thực hiện | • `docker-compose.local.yml` (4 healthchecks)<br>• `docker/trino/etc/catalog/iceberg.properties`<br>• `scripts/init_local_env.py` (S3, DDB, SQS FIFO)<br>• `src/lambda/ingestor/webhook_handler.py`<br>• `test_webhook_handler.py` (Passed) | **Bắt đầu D6 (Tuần 2):** Xây dựng Lambda Reconciliation Job đọc danh sách mã đơn mở từ DynamoDB Watchlist. |
| **👤 Member 2**<br>*(Serving & State Machine)* | 🟢 **Đang ở D9**<br>(Xong Tuần 1 + 60% Tuần 2) | **D1 → D8: ĐÃ XONG**<br>D9: Đang thực hiện | • `transformer.py` (7 terminal states, `delivery_attempt` từ `log`, `phase_rank`)<br>• `ddb_writer.py` (Two-level Hierarchical Condition Expression)<br>• `test_transformer.py` & `test_ddb_writer.py` (Passed) | **Bắt đầu D9 & D10:** Kiểm chứng routing Stale trả về `DUPLICATE` (no-op), chỉ malformed payload mới vào DLQ; đóng gói CDK `ProcessingStack`. |
| **👤 Member 3**<br>*(Lakehouse & Athena SQL)* | 🟡 **Đang ở D5**<br>(Mã SQL xong, cần nạp data) | **D1 → D4, D6 → D8, D10: ĐÃ XONG MÃ NGUỒN SQL** | • `sql/01_create_iceberg_tables.sql`<br>• `sql/02_merge_order_events.sql`<br>• `sql/03_merge_orders.sql`<br>• `sql/04_maintenance.sql` | **Bắt đầu D5 & D9:** Nạp tập Bronze Parquet mẫu vào MinIO/S3 để chạy thử SQL trên Trino; viết câu lệnh SQL tổng hợp bảng `gold.delivery_kpis` (D9). |
| **👤 Member 4**<br>*(Security, Masking & Luật 91)* | 🟡 **Đang ở D2**<br>(Pháp lý & View xong, cần CDK) | **D1, D5, D6: ĐÃ XONG**<br>D2 → D4: Đang thực hiện | • `docs/adr/006-data-privacy-retention-under-law-91-2025.md`<br>• `sql/05_create_masked_views.sql` (Hỗ trợ cả `REDACTED`) | **Bắt đầu D2, D3, D4:** Kiểm tra/cấu hình AWS KMS Customer Managed Key (CMK), S3 Lifecycle (30 ngày hot → Glacier) và tài liệu IAM Role Matrix. |
| **👤 Member 5**<br>*(QA, Chaos Testing & BI)* | 🟢 **Đang ở D5**<br>(Xong 80% Tuần 1) | **D1 → D4: ĐÃ XONG**<br>D5: Đang thực hiện | • `scripts/simulate_webhook.py` (HMAC signer, 10k generator, clock-skew & >=500 out-of-order)<br>• `run_tests.py` (Harness 69/69 test cases) | **Bắt đầu D5 & Tuần 2 (D6 → D10):** Thiết kế layout QuickSight 4 KPIs (D5) và viết bộ 6 kịch bản Chaos Test (`TC-01` → `TC-06`). |

---

## I. MA TRẬN 5 DOMAIN ĐỘC LẬP & HỢP ĐỒNG GIAO TIẾP (CONTRACTS)

```
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                               KIẾN TRÚC 5 DOMAIN ĐỘC LẬP (KHÔNG BỊ BLOCK)                     │
├─────────────────┬──────────────────────────────────┬─────────────────────────────────────────┤
│ Thành viên      │ Module Chuyên Trách              │ Cơ Chế Độc Lập Tuyệt Đối                │
├─────────────────┼──────────────────────────────────┼─────────────────────────────────────────┤
│ **MEMBER 1**    │ **Hạ Tầng, Test Harness & Webhook**│ Tự tạo Mock Webhook Event để test Ingest│
│ (Tech Lead)     │ (Docker, LocalStack, Trino, SQS) │ mà không phụ thuộc hạ nguồn.            │
├─────────────────┼──────────────────────────────────┼─────────────────────────────────────────┤
│ **MEMBER 2**    │ **Serving & State Machine**       │ Nhận input từ file JSON đơn mẫu, test   │
│                 │ (DynamoDB Orders, ddb_writer)    │ độc lập qua `FakeTable` và LocalStack.  │
├─────────────────┼──────────────────────────────────┼─────────────────────────────────────────┤
│ **MEMBER 3**    │ **Lakehouse Core & Athena SQL**   │ Chạy SQL `MERGE INTO` trên Trino/MinIO  │
│                 │ (Iceberg, Step Functions, Vacuum)│ dựa trên tập file Bronze Parquet mẫu.   │
├─────────────────┼──────────────────────────────────┼─────────────────────────────────────────┤
│ **MEMBER 4**    │ **Security, Masking & Luật 91**  │ Viết KMS, IAM, Athena Masking View DDL  │
│                 │ (PII Protection, Right to Forget)│ và S3 Lifecycle độc lập với code app.   │
├─────────────────┼──────────────────────────────────┼─────────────────────────────────────────┤
│ **MEMBER 5**    │ **Chaos Testing, Recon & BI**    │ Viết Generator sinh 10k đơn mẫu, test   │
│                 │ (Data Simulator, QuickSight KPI) │ hộp đen (Black-box) và dựng Dashboard.  │
└─────────────────┴──────────────────────────────────┴─────────────────────────────────────────┘
```

---

## II. TIMELINE CHI TIẾT TỪNG NGÀY CHO TỪNG THÀNH VIÊN (DAY-BY-DAY WITH CHECKBOXES)

---

### 👤 MEMBER 1: TECH LEAD (HẠ TẦNG, TEST HARNESS, CI/CD & WEBHOOK INGESTION)
*Sở hữu: `docker-compose.local.yml`, `scripts/init_local_env.py`, `.github/workflows/`, `src/lambda/ingestor/`*

#### TUẦN 1 (10/11/2026 – 16/11/2026): DỰNG SÂN CHƠI LOCAL & XÂY DỰNG WEBHOOK RECEIVER
* [x] **Ngày 1 (D1 - Thứ Ba, 10/11/2026):** [ĐÃ XONG SẴN] Dựng môi trường Docker Compose chuẩn Trino + MinIO + LocalStack + Iceberg REST.
  * *Tiêu chí hoàn thành (Check):* Chạy `docker compose -f docker-compose.local.yml up -d` $\to$ Cả 4 container healthy.
* [x] **Ngày 2 (D2 - Thứ Tư, 11/11/2026):** [ĐÃ XONG SẴN] Viết script `scripts/init_local_env.py` tự động khởi tạo S3 buckets, DynamoDB, SQS trên LocalStack.
  * *Tiêu chí hoàn thành (Check):* Chạy `python3 scripts/init_local_env.py` $\to$ Trả về `All local resources initialized successfully!`.
* [x] **Ngày 3 (D3 - Thứ Năm, 12/11/2026):** [ĐÃ XONG SẴN] Xây dựng module xác thực chữ ký HMAC-SHA256 (`src/lambda/ingestor/webhook_handler.py`).
  * *Tiêu chí hoàn thành (Check):* Chạy `python3 -m pytest src/lambda/ingestor/tests/test_webhook_handler.py -k verify_signature` $\to$ PASSED.
* [x] **Ngày 4 (D4 - Thứ Sáu, 13/11/2026):** [ĐÃ XONG SẴN] Xây dựng logic chuẩn hóa payload Webhook GHN (`create`, `switch_status`, `update_weight`).
  * *Tiêu chí hoàn thành (Check):* Unit test `test_parse_webhook_payload_pascal_case` $\to$ PASSED.
* [x] **Ngày 5 (D5 - Thứ Hai, 16/11/2026):** [ĐÃ XONG SẴN] Tích hợp ghi S3 Raw Backup và đẩy message vào SQS FIFO Queue (`MessageGroupId=order_code`).
  * *Tiêu chí hoàn thành (Check):* Chạy test giả lập `test_lambda_handler_success` $\to$ SQS nhận message có `MessageGroupId`.

#### TUẦN 2 (17/11/2026 – 23/11/2026): RECONCILIATION JOB & CHỐNG CHẠY TRÙNG LẶP (FENCING LEASE)
* [ ] **Ngày 6 (D6 - Thứ Ba, 17/11/2026):** Xây dựng Lambda Reconciliation Job: Đọc danh sách mã đơn mở từ DynamoDB Watchlist.
  * *Tiêu chí hoàn thành (Check):* Gọi hàm `_scan_watchlist()` trả về đúng danh sách đơn chưa terminal.
* [ ] **Ngày 7 (D7 - Thứ Tư, 18/11/2026):** Viết logic đối soát bù giờ: So khớp trạng thái GHN trả về với trạng thái hiện tại trong DB.
  * *Tiêu chí hoàn thành (Check):* Bù đắp thành công 1 đơn bị giả lập mất Webhook vào SQS.
* [ ] **Ngày 8 (D8 - Thứ Năm, 19/11/2026):** Cài đặt cơ chế Fencing Lease trên DynamoDB: `expires_at < :now` và tăng đơn điệu `fencing_token`.
  * *Tiêu chí hoàn thành (Check):* Viết test 2 process cùng tranh chấp lease $\to$ Chỉ 1 process thắng.
* [ ] **Ngày 9 (D9 - Thứ Sáu, 20/11/2026):** Tích hợp bộ lọc mã băm `batch_hash = MD5(payload)` để short-circuit khi API không có cập nhật mới.
  * *Tiêu chí hoàn thành (Check):* Bắn 2 payload giống nhau liên tiếp $\to$ Lần 2 dừng ngay (no-op, 0 write S3).
* [ ] **Ngày 10 (D10 - Thứ Hai, 23/11/2026):** Đóng gói CDK IngestionStack: Khai báo API Gateway, SQS FIFO và Ingestor Lambda.
  * *Tiêu chí hoàn thành (Check):* Chạy `cdk synth IngestionStack` thành công, không có lỗi binding.

#### TUẦN 3 (24/11/2026 – 30/11/2026): TỐI ƯU HÓA PRODUCTION, ALERTS & DEPLOY AWS THẬT
* [ ] **Ngày 11 (D11 - Thứ Ba, 24/11/2026):** Cấu hình CloudWatch Alarms: Cảnh báo khi Webhook Receiver bị lỗi 5xx hoặc SQS DLQ có message.
  * *Tiêu chí hoàn thành (Check):* Kích hoạt mock error $\to$ CloudWatch Alarm chuyển trạng thái `ALARM`.
* [ ] **Ngày 12 (D12 - Thứ Tư, 25/11/2026):** Triển khai API Gateway và Webhook Receiver lên AWS Staging thật.
  * *Tiêu chí hoàn thành (Check):* Dùng `curl -X POST https://<api-id>.execute-api...` nhận HTTP 200 `{"status": "ok"}`.
* [ ] **Ngày 13 (D13 - Thứ Năm, 26/11/2026):** Test đối soát Reconciliation Job chạy tự động qua EventBridge lúc 03:00 AM trên AWS.
  * *Tiêu chí hoàn thành (Check):* Trigger thủ công EventBridge Rule $\to$ Lambda Reconciler chạy thành công.
* [ ] **Ngày 14 (D14 - Thứ Sáu, 27/11/2026):** Kiểm tra End-to-End độ tươi dữ liệu Ingestion: Bắn webhook $\to$ Message xuất hiện ở SQS trong $< 1$ giây.
  * *Tiêu chí hoàn thành (Check):* Đo latency từ client đến SQS $< 1000\text{ms}$.
* [ ] **Ngày 15 (D15 - Thứ Hai, 30/11/2026):** Đóng gói tài liệu vận hành Ingestion (Runbook API keys, Secrets rotation) và bàn giao.
  * *Tiêu chí hoàn thành (Check):* Tài liệu `docs/runbook.md` có đầy đủ quy trình xử lý sự cố Ingestion.

---

### 👤 MEMBER 2: REAL-TIME SERVING & STATE MACHINE ENGINEER
*Sở hữu: `src/lambda/processor/ddb_writer.py`, `src/lambda/processor/transformer.py`, DynamoDB Orders*

#### TUẦN 1 (10/11/2026 – 16/11/2026): MÔ HÌNH HÓA ĐƠN HÀNG & PHÂN TẦNG TRẠNG THÁI
* [x] **Ngày 1 (D1 - Thứ Ba, 10/11/2026):** [ĐÃ XONG SẴN] Định nghĩa cấu trúc Item DynamoDB: `pk=ORDER#{code}`, `sk={created_date}`, `is_terminal`, `delivery_attempt`.
  * *Tiêu chí hoàn thành (Check):* File sample JSON item chứa đầy đủ các trường trên.
* [x] **Ngày 2 (D2 - Thứ Tư, 11/11/2026):** [ĐÃ XONG SẴN] Nâng cấp `transformer.py`: Phân loại 7 Terminal States (`delivered`, `returned`, `cancelled`, v.v.).
  * *Tiêu chí hoàn thành (Check):* Chạy `test_transformer.py` $\to$ Cả 7 trạng thái đều có `is_terminal = True`.
* [x] **Ngày 3 (D3 - Thứ Năm, 12/11/2026):** [ĐÃ XONG SẴN] Cài đặt logic tính `delivery_attempt` từ mảng lịch sử `log`: $1 + \text{số lần delivery\_fail trước đó}$.
  * *Tiêu chí hoàn thành (Check):* Đơn có 1 lần fail trước đó $\to$ Lần phát kế tiếp mang `delivery_attempt = 2`.
* [x] **Ngày 4 (D4 - Thứ Sáu, 13/11/2026):** [ĐÃ XONG SẴN] Xử lý an toàn các trạng thái lạ (`unknown_state`) không có trong từ điển chuẩn hóa.
  * *Tiêu chí hoàn thành (Check):* Gặp trạng thái lạ $\to$ Gán nhãn `unknown_state`, không quăng ngoại lệ (exception).
* [x] **Ngày 5 (D5 - Thứ Hai, 16/11/2026):** [ĐÃ XONG SẴN] Đảm bảo chuyển đổi số thực sang `Decimal` cho toàn bộ cước phí và khối lượng (`boto3` compatibility).
  * *Tiêu chí hoàn thành (Check):* `test_numeric_fields_are_decimal` $\to$ PASSED.

#### TUẦN 2 (17/11/2026 – 23/11/2026): CÀI ĐẶT ĐỒ THỊ TRẠNG THÁI & CHỐNG GHI ĐÈ SAI THỨ TỰ
* [x] **Ngày 6 (D6 - Thứ Ba, 17/11/2026):** [ĐÃ XONG SẴN] Cài đặt biểu thức điều kiện hai cấp (Hierarchical Condition Expression) trong `OrderWriter.put()`.
  * *Tiêu chí hoàn thành (Check):* `ConditionExpression` bảo vệ `is_terminal` và so sánh `delivery_attempt`.
* [x] **Ngày 7 (D7 - Thứ Tư, 18/11/2026):** [ĐÃ XONG SẴN] Viết unit test ca kiểm thử giao lại lần 2 (`test_retry_loop_second_attempt_succeeds`).
  * *Tiêu chí hoàn thành (Check):* `delivering (attempt 2)` ghi đè thành công lên `delivery_fail (attempt 1)`.
* [x] **Ngày 8 (D8 - Thứ Năm, 19/11/2026):** [ĐÃ XONG SẴN] Viết unit test ca kiểm thử bảo vệ Terminal (`test_terminal_order_rejects_earlier_delivery_fail`).
  * *Tiêu chí hoàn thành (Check):* Đơn đã `delivered` từ chối bản ghi `delivery_fail` cũ đến muộn.
* [ ] **Ngày 9 (D9 - Thứ Sáu, 20/11/2026):** Xử lý định tuyến bản ghi: Stale/Duplicate trả về `DUPLICATE` (No-op), chỉ Malformed JSON mới vào DLQ.
  * *Tiêu chí hoàn thành (Check):* Bản ghi stale không làm tăng message count trong SQS DLQ.
* [ ] **Ngày 10 (D10 - Thứ Hai, 23/11/2026):** Đóng gói CDK ProcessingStack: Khai báo Lambda Processor và DynamoDB Orders Table.
  * *Tiêu chí hoàn thành (Check):* Chạy `cdk synth ProcessingStack` thành công.

#### TUẦN 3 (24/11/2026 – 30/11/2026): TỐI ƯU HÓA READ LATENCY & BENCHMARK LOCALSTACK / AWS
* [ ] **Ngày 11 (D11 - Thứ Ba, 24/11/2026):** Tối ưu hóa truy vấn điểm Point-Lookup: Đo latency đọc qua `GetItem` trên LocalStack DynamoDB.
  * *Tiêu chí hoàn thành (Check):* Thời gian phản hồi `GetItem` đạt $< 10\text{ms}$.
* [ ] **Ngày 12 (D12 - Thứ Tư, 25/11/2026):** Triển khai DynamoDB Orders Table lên AWS thật với chế độ On-demand Billing.
  * *Tiêu chí hoàn thành (Check):* Bảng được tạo trên AWS console với Primary Key `pk` (String) và `sk` (String).
* [ ] **Ngày 13 (D13 - Thứ Năm, 26/11/2026):** Test tích hợp: Lambda Processor tiêu thụ message từ SQS FIFO và ghi vào DynamoDB trên AWS.
  * *Tiêu chí hoàn thành (Check):* Đẩy message vào SQS FIFO $\to$ Item xuất hiện ngay trên DynamoDB table.
* [ ] **Ngày 14 (D14 - Thứ Sáu, 27/11/2026):** Chạy bài test tải 100 writes/giây vào DynamoDB Writer đảm bảo không bị lỗi `ProvisionedThroughputExceeded`.
  * *Tiêu chí hoàn thành (Check):* 1,000 writes liên tục không có lỗi throttle.
* [ ] **Ngày 15 (D15 - Thứ Hai, 30/11/2026):** Hoàn thiện tài liệu cấu trúc dữ liệu DynamoDB và bàn giao.
  * *Tiêu chí hoàn thành (Check):* Đóng gói xong tài liệu thiết kế Primary Key & Secondary Indexes.

---

### 👤 MEMBER 3: DATA LAKEHOUSE & ATHENA ICEBERG ENGINEER
*Sở hữu: `sql/`, Apache Iceberg Catalog, AWS Step Functions, Athena Workgroups*

#### TUẦN 1 (10/11/2026 – 16/11/2026): KHỞI TẠO BẢNG ICEBERG DDL & CẤU HÌNH TRINO LOCAL
* [x] **Ngày 1 (D1 - Thứ Ba, 10/11/2026):** [ĐÃ XONG SẴN DDL] Viết DDL tạo database `ghn_analytics` trên Trino Engine / MinIO Catalog.
  * *Tiêu chí hoàn thành (Check):* Thực thi `CREATE DATABASE ghn_analytics` thành công trên Trino container.
* [x] **Ngày 2 (D2 - Thứ Tư, 11/11/2026):** [ĐÃ XONG SẴN DDL] Viết DDL tạo bảng `silver.order_events` dạng Iceberg, phân vùng theo `days(created_date)`.
  * *Tiêu chí hoàn thành (Check):* Chạy `sql/01_create_iceberg_tables.sql` $\to$ Bảng `order_events` được tạo.
* [x] **Ngày 3 (D3 - Thứ Năm, 12/11/2026):** [ĐÃ XONG SẴN DDL] Viết DDL tạo bảng `silver.orders` dạng Iceberg, phân vùng `days(created_date)` và `bucket(order_code, 16)`.
  * *Tiêu chí hoàn thành (Check):* Bảng `orders` được tạo với thuộc tính `table_type='ICEBERG'`.
* [x] **Ngày 4 (D4 - Thứ Sáu, 13/11/2026):** [ĐÃ XONG SẴN DDL] Viết DDL tạo bảng `gold.delivery_kpis` để lưu trữ các chỉ số tổng hợp theo ngày.
  * *Tiêu chí hoàn thành (Check):* Bảng `gold.delivery_kpis` được tạo thành công trên Trino.
* [ ] **Ngày 5 (D5 - Thứ Hai, 16/11/2026):** Nạp tập dữ liệu Bronze Parquet mẫu vào MinIO để chuẩn bị thử nghiệm các câu lệnh DML.
  * *Tiêu chí hoàn thành (Check):* Lệnh `SELECT count(*) FROM bronze_staging` trả về 1,000 dòng mẫu.

#### TUẦN 2 (17/11/2026 – 23/11/2026): VIẾT VÀ KIỂM CHỨNG CÂU LỆNH ATHENA SQL `MERGE INTO`
* [x] **Ngày 6 (D6 - Thứ Ba, 17/11/2026):** [ĐÃ XONG SẴN SQL] Viết và kiểm thử lệnh `MERGE INTO` cho bảng `silver.order_events` (chống duplicate bằng `event_id`).
  * *Tiêu chí hoàn thành (Check):* Chạy `sql/02_merge_order_events.sql` $\to$ Chạy lặp lại 2 lần số dòng không đổi.
* [x] **Ngày 7 (D7 - Thứ Tư, 18/11/2026):** [ĐÃ XONG SẴN SQL] Viết biểu thức Window Function `ROW_NUMBER()` Pre-deduplication theo thứ tự ưu tiên của State Machine.
  * *Tiêu chí hoàn thành (Check):* 1 đơn có 3 trạng thái trong batch $\to$ Query chỉ chọn ra 1 bản ghi ưu tiên nhất.
* [x] **Ngày 8 (D8 - Thứ Năm, 19/11/2026):** [ĐÃ XONG SẴN SQL] Viết câu lệnh `MERGE INTO` hoàn chỉnh cho bảng `silver.orders` (kèm Partition Pruning).
  * *Tiêu chí hoàn thành (Check):* Chạy `sql/03_merge_orders.sql` trên Trino $\to$ Trạng thái cập nhật chuẩn xác.
* [ ] **Ngày 9 (D9 - Thứ Sáu, 20/11/2026):** Viết câu lệnh Athena SQL tổng hợp dữ liệu từ `silver` sang `gold.delivery_kpis` theo ngày.
  * *Tiêu chí hoàn thành (Check):* Bảng Gold xuất hiện dòng tổng hợp cho ngày hôm đó.
* [x] **Ngày 10 (D10 - Thứ Hai, 23/11/2026):** [ĐÃ XONG SẴN SQL] Viết script bảo trì: Nén tệp `OPTIMIZE BIN_PACK` và dọn snapshot hết hạn `VACUUM`.
  * *Tiêu chí hoàn thành (Check):* Chạy `sql/04_maintenance.sql` trên Trino thành công.

#### TUẦN 3 (24/11/2026 – 30/11/2026): ĐIỀU PHỐI STEP FUNCTIONS & DEPLOY PRODUCTION ATHENA
* [ ] **Ngày 11 (D11 - Thứ Ba, 24/11/2026):** Khai báo bảng Iceberg trên AWS Glue Data Catalog bằng AWS CDK (`AnalyticsStack`).
  * *Tiêu chí hoàn thành (Check):* Bảng `ghn_analytics.orders` xuất hiện trên AWS Glue Console.
* [ ] **Ngày 12 (D12 - Thứ Tư, 25/11/2026):** Thiết lập **AWS Step Functions**: Điều phối tuần tự: `MERGE events` $\to$ `MERGE orders` $\to$ `Daily Gold`.
  * *Tiêu chí hoàn thành (Check):* Chạy Execution trên Step Functions $\to$ Cả 3 state đều chuyển sang màu xanh lá (Succeeded).
* [ ] **Ngày 13 (D13 - Thứ Năm, 26/11/2026):** Thiết lập EventBridge Cron kích hoạt `OPTIMIZE` và `VACUUM` vào 02:00 AM hàng ngày.
  * *Tiêu chí hoàn thành (Check):* EventBridge Rule được cấu hình với target là Step Functions Maintenance.
* [ ] **Ngày 14 (D14 - Thứ Sáu, 27/11/2026):** Đo lường lượng dữ liệu quét (Data Scanned Bytes) trên Athena Workgroup: Đảm bảo $< 50\text{MB}$/query.
  * *Tiêu chí hoàn thành (Check):* Athena Query History ghi nhận Data Scanned đạt định mức tối ưu chi phí.
* [ ] **Ngày 15 (D15 - Thứ Hai, 30/11/2026):** Đóng gói toàn bộ script SQL và cấu hình Athena Workgroup bàn giao cho nhóm.
  * *Tiêu chí hoàn thành (Check):* Thư mục `sql/` có đầy đủ 5 file SQL chuẩn mực kèm tài liệu hướng dẫn.

---

### 👤 MEMBER 4: DATA GOVERNANCE, SECURITY & COMPLIANCE ENGINEER
*Sở hữu: `docs/adr/006-*.md`, AWS KMS CMK, Athena Masking Views, S3 Lifecycle*

#### TUẦN 1 (10/11/2026 – 16/11/2026): THIẾT KẾ CHÍNH SÁCH BẢO MẬT & LUẬT 91/2025/QH15
* [x] **Ngày 1 (D1 - Thứ Ba, 10/11/2026):** [ĐÃ XONG SẴN] Nghiên cứu các điều khoản lưu trữ bưu chính và nghĩa vụ lưu trữ chứng từ kế toán (1–5 năm).
  * *Tiêu chí hoàn thành (Check):* Hoàn thiện bản đặc tả pháp lý trong `docs/adr/006-data-privacy-retention-under-law-91-2025.md`.
* [ ] **Ngày 2 (D2 - Thứ Tư, 11/11/2026):** Thiết lập cấu hình mã hóa lưu trữ tại chỗ (At-Rest) bằng **AWS KMS Customer Managed Key (CMK)**.
  * *Tiêu chí hoàn thành (Check):* Viết CDK định nghĩa Key với chính sách `enable_key_rotation=True`.
* [ ] **Ngày 3 (D3 - Thứ Năm, 12/11/2026):** Cấu hình chính sách S3 Lifecycle Rules: S3 Raw nóng 30 ngày, tự động chuyển Glacier Archive 1 năm.
  * *Tiêu chí hoàn thành (Check):* Lifecycle Configuration JSON được kiểm chứng hợp lệ trên AWS.
* [ ] **Ngày 4 (D4 - Thứ Sáu, 13/11/2026):** Phân tích ranh giới phân quyền: Nhóm BI (chỉ xem dữ liệu masked) vs Nhóm CSKH (xem PII giải mã).
  * *Tiêu chí hoàn thành (Check):* Tài liệu phân quyền IAM Role Matrix cho từng nhóm người dùng.
* [x] **Ngày 5 (D5 - Thứ Hai, 16/11/2026):** [ĐÃ XONG SẴN] Thiết kế cấu trúc SQL View che mờ: `to_phone` $\to$ `090*****89`, `to_name` $\to$ `N***`.
  * *Tiêu chí hoàn thành (Check):* Câu lệnh SQL `CONCAT(SUBSTR(...))` được kiểm chứng trên Trino local.

#### TUẦN 2 (17/11/2026 – 23/11/2026): XÂY DỰNG MASKING VIEW & QUY TRÌNH QUYỀN ĐƯỢC XÓA (RIGHT TO FORGET)
* [x] **Ngày 6 (D6 - Thứ Ba, 17/11/2026):** [ĐÃ XONG SẴN DDL] Viết file DDL tạo View che mờ bảo mật `sql/05_create_masked_views.sql`.
  * *Tiêu chí hoàn thành (Check):* Chạy script $\to$ View `ghn_analytics.orders_masked` được tạo thành công.
* [ ] **Ngày 7 (D7 - Thứ Tư, 18/11/2026):** Thử nghiệm truy vấn trên View: Đảm bảo số điện thoại và tên hiển thị dạng dấu sao `*****`.
  * *Tiêu chí hoàn thành (Check):* Lệnh `SELECT to_phone_masked FROM orders_masked` trả về dữ liệu che mờ 100%.
* [ ] **Ngày 8 (D8 - Thứ Năm, 19/11/2026):** Viết script thực thi quyền được xóa dữ liệu cá nhân (Right to be Forgotten) trên bảng `orders`:
  * *Tiêu chí hoàn thành (Check):* Chạy `UPDATE orders SET to_phone='REDACTED' WHERE order_code='...'` thành công.
* [ ] **Ngày 9 (D9 - Thứ Sáu, 20/11/2026):** Viết script kiểm tra rò rỉ PII: Quét toàn bộ các cột trong Lakehouse xem có trường PII nào chưa che mờ.
  * *Tiêu chí hoàn thành (Check):* Script scan trả về 0 phát hiện vi phạm.
* [ ] **Ngày 10 (D10 - Thứ Hai, 23/11/2026):** Đóng gói CDK BaseStack: Tích hợp KMS Key và S3 Lifecycle Policies vào hạ tầng chung.
  * *Tiêu chí hoàn thành (Check):* Chạy `cdk synth BaseStack` thành công.

#### TUẦN 3 (24/11/2026 – 30/11/2026): AUDITING BẢO MẬT & KIỂM TRA PHÂN QUYỀN TRÊN AWS THẬT
* [ ] **Ngày 11 (D11 - Thứ Ba, 24/11/2026):** Triển khai khóa KMS CMK và S3 Lifecycle lên tài khoản AWS chính thức.
  * *Tiêu chí hoàn thành (Check):* S3 Bucket trên AWS Console hiển thị trạng thái `Encrypted with AWS KMS`.
* [ ] **Ngày 12 (D12 - Thứ Tư, 25/11/2026):** Cấu hình quyền truy cập Athena Workgroup: Giới hạn nhóm Analyst chỉ được query trên schema `orders_masked`.
  * *Tiêu chí hoàn thành (Check):* Dùng IAM role Analyst thử query bảng gốc `orders` $\to$ Nhận lỗi `Access Denied`.
* [ ] **Ngày 13 (D13 - Thứ Năm, 26/11/2026):** Thử nghiệm quy trình xóa PII trên AWS thật: Thực thi lệnh Redaction $\to$ Dữ liệu trên View chuyển thành `REDACTED`.
  * *Tiêu chí hoàn thành (Check):* Tra cứu lại mã đơn đó trên Athena $\to$ Thông tin cá nhân đã bị xóa.
* [ ] **Ngày 14 (D14 - Thứ Sáu, 27/11/2026):** Rà soát log CloudWatch của các Lambda functions: Đảm bảo không ghi đè số điện thoại dạng rõ ra log.
  * *Tiêu chí hoàn thành (Check):* CloudWatch Log Insights không tìm thấy pattern số điện thoại 10 số.
* [ ] **Ngày 15 (D15 - Thứ Hai, 30/11/2026):** Hoàn thiện báo cáo Tuân thủ Bảo mật & Pháp lý (Security Compliance Report) và bàn giao.
  * *Tiêu chí hoàn thành (Check):* Báo cáo đánh giá rủi ro an ninh thông tin đạt phê duyệt.

---

### 👤 MEMBER 5: QA, CHAOS TESTING & BI ANALYTICS ENGINEER
*Sở hữu: `scripts/simulate_webhook.py`, Bộ 6 Chaos Tests, QuickSight Dashboard*

#### TUẦN 1 (10/11/2026 – 16/11/2026): XÂY DỰNG MOCK DATA GENERATOR & THIẾT LẬP HARNESS
* [x] **Ngày 1 (D1 - Thứ Ba, 10/11/2026):** [ĐÃ XONG SẴN CODE] Viết script `scripts/simulate_webhook.py`: Giả lập bắn Webhook GHN với chữ ký HMAC hợp lệ.
  * *Tiêu chí hoàn thành (Check):* Bắn 1 request vào Webhook Receiver $\to$ Nhận phản hồi HTTP 200.
* [x] **Ngày 2 (D2 - Thứ Tư, 11/11/2026):** [ĐÃ XONG SẴN CODE] Mở rộng generator: Sinh tập dữ liệu 10,000 đơn hàng có đầy đủ phân bố trạng thái (giao thành công, hoàn, hủy).
  * *Tiêu chí hoàn thành (Check):* Chạy `python3 scripts/simulate_webhook.py generate --output data/sample/10k_orders.jsonl` xuất file chuẩn định dạng.
* [x] **Ngày 3 (D3 - Thứ Năm, 12/11/2026):** [ĐÃ XONG SẴN CODE] Bơm các ca dị tật vào dữ liệu mẫu: Đơn hàng bị lệch đồng hồ (clock skew) và đơn đổi trạng thái đảo lộn.
  * *Tiêu chí hoàn thành (Check):* File dữ liệu mẫu có tối thiểu 500 bản ghi out-of-order (`out_of_order_orders: 666`, `clock_skew_orders: 500`).
* [x] **Ngày 4 (D4 - Thứ Sáu, 13/11/2026):** [ĐÃ XONG SẴN HARNESS] Viết khung kiểm thử tự động (Test Runner) chạy toàn bộ bộ test cục bộ không cần mạng.
  * *Tiêu chí hoàn thành (Check):* Lệnh `python3 run_tests.py` chạy qua tất cả các test suite hiện có (69/69 passed).
* [ ] **Ngày 5 (D5 - Thứ Hai, 16/11/2026):** Thiết kế mockup giao diện Dashboard QuickSight với 4 biểu đồ KPI cốt lõi.
  * *Tiêu chí hoàn thành (Check):* Hoàn thành bản vẽ phác thảo Layout Dashboard và danh sách Metric cần tính.

#### TUẦN 2 (17/11/2026 – 23/11/2026): XÂY DỰNG BỘ 6 KỊCH BẢN CHAOS TEST & SCRIPT ĐỐI SOÁT
* [ ] **Ngày 6 (D6 - Thứ Ba, 17/11/2026):** Cài đặt **TC-01 (Đối soát nội dung):** Bơm 10,000 đơn mẫu $\to$ Kiểm tra Checksum MD5 và đối soát số lượng.
  * *Tiêu chí hoàn thành (Check):* Số lượng bản ghi `Count(GHN) == Count(DynamoDB)` lệch = 0.
* [ ] **Ngày 7 (D7 - Thứ Tư, 18/11/2026):** Cài đặt **TC-02 (Vòng lặp giao lại lần 2):** Bơm chuỗi `delivering (1)` $\to$ `fail (1)` $\to$ `delivering (2)` $\to$ `delivered`.
  * *Tiêu chí hoàn thành (Check):* DynamoDB ghi nhận trạng thái cuối cùng là `delivered (attempt 2)`.
* [ ] **Ngày 8 (D8 - Thứ Năm, 19/11/2026):** Cài đặt **TC-03 (Chống Out-of-Order Stale):** Bơm `delivered` lúc 11:00, sau đó bơm `delivering` (1) lúc 11:05.
  * *Tiêu chí hoàn thành (Check):* Trạng thái `delivered` được bảo vệ tuyệt đối, không bị ghi đè.
* [ ] **Ngày 9 (D9 - Thứ Sáu, 20/11/2026):** Cài đặt **TC-04 (Idempotent Event Log):** Chạy lặp lại 1 batch với cùng `event_id`.
  * *Tiêu chí hoàn thành (Check):* Bảng `order_events` không bị tăng số dòng (không bị trùng lặp).
* [ ] **Ngày 10 (D10 - Thứ Hai, 23/11/2026):** Cài đặt **TC-05 (Fencing Lease Contention) & TC-06 (Compaction Small Files).**
  * *Tiêu chí hoàn thành (Check):* Lệnh `OPTIMIZE` gộp 50 micro-files về đúng 1 file tiêu chuẩn mà không downtime.

#### TUẦN 3 (24/11/2026 – 30/11/2026): TRIỂN KHAI AMAZON QUICKSIGHT BI & NGHIỆM THU E2E
* [ ] **Ngày 11 (D11 - Thứ Ba, 24/11/2026):** Chạy toàn bộ Bộ 6 Kịch bản Chaos Test trên môi trường AWS Staging thật.
  * *Tiêu chí hoàn thành (Check):* Cả 6 test case đều hiển thị kết quả `PASSED (6/6)`.
* [ ] **Ngày 12 (D12 - Thứ Tư, 25/11/2026):** Kết nối Amazon Athena v3 với Amazon QuickSight qua Athena Workgroup.
  * *Tiêu chí hoàn thành (Check):* QuickSight đọc được danh sách bảng trong database `ghn_analytics`.
* [ ] **Ngày 13 (D13 - Thứ Năm, 26/11/2026):** Dựng 4 visual biểu đồ nghiệp vụ trên QuickSight:
  * 1. *First-Attempt Delivery Rate (FADR %)* theo ngày.
  * 2. *Return Rate (%)* theo Mature Cohort.
  * 3. *Average Pick-to-Delivery Lead-Time (Hours)*.
  * 4. *Báo cáo tổng hợp cước phí và COD theo tuyến tỉnh thành*.
  * *Tiêu chí hoàn thành (Check):* Dashboard hiển thị số liệu trực quan, có bộ lọc (filter) theo ngày và tỉnh thành.
* [ ] **Ngày 14 (D14 - Thứ Sáu, 27/11/2026):** Thực hiện bài kiểm thử đối soát cuối cùng: `Count(GHN) == Count(DynamoDB) == Count(Iceberg Orders)`.
  * *Tiêu chí hoàn thành (Check):* Script `reconcile_pipeline.py` trả về độ lệch `Drift = 0.00%`.
* [ ] **Ngày 15 (D15 - Thứ Hai, 30/11/2026):** Đóng gói báo cáo kết quả kiểm thử (Test Report), xuất Dashboard sang PDF và bàn giao nghiệm thu.
  * *Tiêu chí hoàn thành (Check):* Slide thuyết trình và biên bản nghiệm thu kỹ thuật hoàn tất.

---

## III. QUY TẮC ĐỘC LẬP & TỰ CHỦ CHO 5 THÀNH VIÊN (ZERO-BLOCKER RULES)

1. **Không chờ đợi API ngoài:** 
   * Member 1, 2, 3, 4, 5 đều dùng dữ liệu mẫu (Sample JSON/Parquet) trong thư mục `data/sample/`. Không ai phải chờ GHN duyệt webhook hay chờ người khác deploy xong mới làm được việc của mình.
2. **Kiểm tra chéo qua Definition of Done (DoD):**
   * Mỗi ngày, từng thành viên chỉ cần mở file checklist này ra, chạy đúng câu lệnh trong phần *(Check)*. Nếu lệnh chạy thành công $\to$ tick vào ô `[x]`.
3. **Tích hợp cuối tuần (Friday Integration):**
   * Chiều thứ Sáu hàng tuần (13/11, 20/11, 27/11), cả 5 thành viên ghép các module lại với nhau thông qua file `docker-compose.local.yml`. Bất kỳ lỗi xung đột nào sẽ được giải quyết ngay trong ngày thứ Sáu trước khi sang tuần mới.
