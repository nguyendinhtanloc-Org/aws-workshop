# GHN staging → local webhook → LocalStack

## Contract đang dùng

Theo [GHN Order Status Callback](https://developer.ghn.dev/en/docs/webhook/callback-order-status), GHN POST JSON có `OrderCode`, `Type` (`create`, `switch_status`, ...), `Time`, `Status` tới URL webhook trên portal staging. Payload mẫu trong tài liệu không có ngày tạo đơn. Portal cho phép **custom headers**, vì vậy cấu hình `X-GHN-Webhook-Token` bằng cùng giá trị riêng tư với `GHN_CALLBACK_SHARED_TOKEN` trong `.env`. GHN không công bố header HMAC `x-ghn-signature` trong contract này; HMAC hiện tại chỉ dùng cho simulator/legacy. GHN coi `2xx` là thành công và có thể gửi lại callback lỗi. Cấu hình portal có thể mất khoảng **15 phút** mới có hiệu lực.

Server xác thực token trước. Nếu thiếu `created_date`, handler gọi [GHN Order Info staging](https://developer.ghn.dev/en/docs/order/info) bằng `OrderCode` để lấy ngày tạo ổn định. Lỗi API trả `503`, để GHN thử lại; không dùng `Time` của callback làm ngày tạo. Handler ghi **byte JSON gốc** vào `raw/webhook-original/...`, bản envelope `{source, ingested_at, records}` mà Processor đang đọc vào `raw/webhook/...`, đăng ký `#WATCH`/`OrderCode` trên DynamoDB, rồi gửi record chuẩn hóa vào SQS FIFO. Ghi S3 hoặc gửi SQS lỗi trả `500`; callback gửi lại có thể tạo thêm object S3 nhưng `registered_at` không bị reset. `MessageDeduplicationId` giới hạn trùng trong cửa sổ FIFO; Processor cần tiếp tục xử lý idempotent.

## Chạy trong PowerShell

Từ thư mục gốc repo:

```powershell
docker compose -f docker-compose.local.yml up -d
python -m pip install -r requirements-dev.txt
python -m pip install -r src/lambda/ingestor/requirements.txt
python scripts/init_local_env.py
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
notepad .env
```

Trong `.env`, điền **token và ShopId staging** cho `GHN_API_TOKEN`, `GHN_SHOP_ID`; giữ `GHN_BASE_URL=https://dev-online-gateway.ghn.vn`. Đặt `GHN_CALLBACK_SHARED_TOKEN` thành chuỗi ngẫu nhiên riêng dài ít nhất 24 ký tự; không dùng placeholder. Giá trị này phải giống hệt giá trị custom header trên portal. Không in token trong terminal, không commit `.env`.

Nếu chưa biết ShopId, trước tiên chỉ điền `GHN_API_TOKEN` staging, rồi chạy `python scripts/test_api.py --endpoint shops`. Lệnh gọi API staging `shop/all` chỉ cần Token và in `ShopId` cùng tên/trạng thái shop; chép ID của shop đang hoạt động vào `GHN_SHOP_ID` trong `.env`. Không dùng kết quả từ tài khoản production.

Mở cửa sổ PowerShell thứ hai:

```powershell
python scripts/serve_webhook_local.py
```

Server chỉ lắng nghe `127.0.0.1:8000`. Kiểm tra ở cửa sổ thứ ba:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
```

Mở **một** tunnel, chọn [ngrok](https://ngrok.com/docs/start) hoặc [Cloudflare Quick Tunnel](https://developers.cloudflare.com/tunnel/get-started/quick-tunnels/):

```powershell
ngrok http 8000
# hoặc
cloudflared tunnel --url http://127.0.0.1:8000
```

Tunnel in ra URL HTTPS. Chỉ forward cổng **8000** của webhook; không forward LocalStack `4566`, MinIO `9000` hay Trino `8080`.

## Thao tác trên GHN Developer Portal staging

1. Đăng nhập **developer.ghn.dev** bằng tài khoản staging. Vào menu tài khoản → **Webhook configuration** → tab **Order**.
2. Đặt URL `https://<tunnel-host>/webhook/ghn` và custom header `X-GHN-Webhook-Token` = giá trị `GHN_CALLBACK_SHARED_TOKEN` trong `.env`. Bật ít nhất `create` và `switch_status` trong **Permissions · Order**. Lưu/Create webhook.
3. Đợi tối đa khoảng 15 phút để cấu hình có hiệu lực. Nếu tunnel cấp hostname mới, cập nhật URL portal và đợi lại.
4. Chạy tạo **một đơn staging** ở cửa sổ riêng. Có thể thử payload trước bằng `--dry-run`. Dùng lại cùng `--client-order-code` khi retry để GHN trả cùng `order_code`, theo [GHN Create Order](https://developer.ghn.dev/en/docs/order/create).

```powershell
python scripts/create_demo_order.py --dry-run --client-order-code STAGING-DEMO-001
python scripts/create_demo_order.py --client-order-code STAGING-DEMO-001
```

Script từ chối gateway production, in `order_code` và ghi cặp mã đơn vào `.local_staging_orders.jsonl` (đã gitignore). Nó thử đăng ký mã đơn vào watchlist **LocalStack**; callback thật vẫn đi qua HTTP server để tạo raw và SQS. Lệnh tạo đơn không chạy khi khởi động server. Staging không tự động chuyển đơn sang `picking`, `delivering` hay `delivered`.

Muốn xác nhận riêng API detail staging có `created_date`, lấy mã đơn vừa in và chạy:

```powershell
$env:GHN_ORDER_CODES = "<ma_don_GHN>"
python scripts/test_api.py --endpoint orders --limit 1
```

## Quan sát cùng một đơn

Trong log server, tìm `order_code=<mã>` cùng `type=create` hoặc `type=switch_status` và `result=200`. Dùng mã đó:

```powershell
python scripts/inspect_local_data.py --order-code <ma_don_GHN>
```

Đối chiếu `OrderCode` trong `raw/webhook-original/...`, `order_code`/`created_at` trong `raw/webhook/...`, item `pk=#WATCH, sk=<mã>`, và SQS `Body.order_code`. Script inspect chỉ đọc tối đa 10 message SQS đang available; FIFO có thể không trả message nếu đang in flight. Việc đọc sẽ tăng receive count, không xóa message. Bucket `curated` và bảng `orders` vẫn trống nếu chưa chạy Processor của Member 2.

**Phân biệt nguồn:** `scripts/demo_webhook.py` gọi handler trực tiếp bằng dữ liệu giả, không đi qua HTTP/tunnel/GHN. Test integration cũng dùng payload mẫu và stub order-detail GHN, dù S3/DynamoDB/SQS thật trong LocalStack. Chỉ log request qua tunnel sau khi tạo đơn trên GHN staging và có mã đơn trùng trong ba nơi mới là bằng chứng callback GHN thật.

## Kiểm thử

```powershell
python -m pytest -o addopts= -p no:cacheprovider src/lambda/ingestor/tests/test_webhook_handler.py -v
$env:LOCALSTACK_ENDPOINT = "http://localhost:4566"
python -m pytest -o addopts= -p no:cacheprovider tests/integration/test_local_ghn_receiver.py -v
```

Integration test tạo tài nguyên riêng trong LocalStack, gửi HTTP tới server loopback, kiểm tra S3/DynamoDB/SQS, callback lặp và lỗi S3/SQS. Nó **không** chứng minh GHN đã gọi callback. Cần portal staging, token staging, tunnel đang chạy và một đơn staging thật để hoàn tất bước đó.

Kết quả tại workspace ngày 17/11/2026: `12 passed` cho 2 lệnh pytest trên; `python scripts/init_local_env.py` xác nhận tài nguyên webhook LocalStack có sẵn; `git diff --check` và `compileall` qua. Chưa chạy callback thật từ GHN staging vì không có bằng chứng portal/tunnel/token staging trong phiên này. Ruff chưa chạy được vì môi trường Python hiện tại chưa cài module `ruff`.


## Update 2026-11-18

Kinesis Data Streams and Firehose have been removed from application code and CDK. Webhook now writes only the normalized `raw/webhook/` envelope, Watchlist and FIFO. The separate original-payload S3 copy is no longer written. Existing objects are retained. Legacy curated/Glue input has no streaming producer; the Athena/Iceberg analytics path still needs implementation. Unit and LocalStack HTTP integration checks: 53 passed.

CDK synth verified: 5 templates, no Kinesis/Firehose resources or stream environment variables.

Local setup update 2026-11-18: init_local_env.py creates only the raw bucket on LocalStack and the warehouse bucket on MinIO. inspect_local_data.py inspects LOCAL_RAW_BUCKET_NAME (default ghn-analytics-local-raw) only. The local curated bucket is no longer part of this workflow.
