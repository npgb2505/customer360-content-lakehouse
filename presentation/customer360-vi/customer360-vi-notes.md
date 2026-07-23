# Customer 360 Content Lakehouse — ghi chú thuyết trình

> Tài liệu này không chiếu. Mỗi mục tương ứng một slide trong `index.html`.

## Slide 1 — Mở đầu

Dự án mô phỏng một nền tảng nội dung số cần hợp nhất log xem và log tìm kiếm. Điểm chính không phải là “đổ dữ liệu vào lake”, mà là tạo ra một chuỗi dữ liệu có thể chạy lại, kiểm tra và giải thích. Stack local dùng PySpark, Airflow, Docker và Power BI; kiến trúc được thiết kế để ánh xạ lên Azure khi có hạ tầng thật.

## Slide 2 — Bài toán

Một công ty nội dung thường có dữ liệu khách hàng, thuê bao, danh mục nội dung, lượt xem và tìm kiếm ở nhiều hệ thống. Nếu ghép trực tiếp trong dashboard, logic bị phân tán và rất khó kiểm soát. Pipeline giải quyết ba vấn đề: hợp nhất dữ liệu, loại bỏ dữ liệu không đáng tin cậy và vận hành theo lịch hoặc khoảng ngày.

## Slide 3 — Quy mô thiết kế

600K watch events và 50K search events mỗi ngày là workload mục tiêu để định hướng partition, grain và mô hình dữ liệu. Bộ demo trong repo nhỏ hơn để chạy nhanh trên laptop. Đây là cách trình bày trung thực: quy mô sản xuất là mục tiêu thiết kế, còn số liệu demo là bằng chứng có thể tái hiện.

## Slide 4 — Kiến trúc tổng thể

Đọc sơ đồ từ trái sang phải. Nguồn dữ liệu đi vào một DAG Airflow. Bronze giữ dữ liệu gần nguyên bản và thêm metadata ingest. Silver khử trùng lặp, kiểm tra khóa ngoại, timestamp và thời lượng; dữ liệu sai được chuyển sang quarantine. Gold tạo bốn data product, sau đó Power BI chỉ đọc các bảng đã được chuẩn hóa. `quality-report.json` là bằng chứng tự động về tính toàn vẹn.

## Slide 5 — Ba chế độ Airflow

- Incremental: xử lý partition mới, là chế độ thường dùng hằng ngày.
- Backfill: chạy lại một khoảng ngày bị thiếu mà không phải viết DAG mới.
- Full reload: tái tạo toàn bộ khi mô hình hoặc quy tắc nghiệp vụ thay đổi.

Điểm thiết kế quan trọng là cùng một DAG và cùng một đường kiểm soát chất lượng được dùng ở cả ba chế độ.

## Slide 6 — Medallion

Bronze ưu tiên traceability: biết bản ghi đến từ đâu và lúc nào. Silver ưu tiên correctness: deduplicate theo ID, kiểm tra quan hệ với customer/content và tách bản ghi lỗi. Gold ưu tiên usability: mỗi bảng có grain ổn định, tên cột rõ và phục vụ một nhóm câu hỏi phân tích cụ thể.

## Slide 7 — Data Quality

Hai phương trình đối soát giúp phát hiện mất dữ liệu hoặc phân loại sai:

`raw = deduplicated + duplicate`

`deduplicated = accepted + quarantined`

Quarantine không có nghĩa là xóa. Bản ghi vẫn được lưu cùng `rejection_reason`, giúp điều tra nguồn lỗi và tái xử lý sau này. Nếu một phương trình không đúng, quality gate phải thất bại thay vì âm thầm xuất báo cáo sai.

## Slide 8 — Bốn data product

`customer_360` tổng hợp hành vi theo khách hàng. `content_kpis` đo hiệu suất nội dung. `search_trends` theo dõi nhu cầu theo ngày. `monthly_search_trends` gom xu hướng để so sánh giữa các tháng. Tách thành nhiều bảng thay vì một “bảng khổng lồ” giúp grain rõ hơn và mô hình Power BI đơn giản hơn.

## Slide 9 — Power BI

Dashboard không nên chứa các phép sửa dữ liệu quan trọng. Những quy tắc như loại duplicate, xử lý khóa ngoại hoặc xác định completion phải nằm trong pipeline và có test. Power BI tập trung vào measures, slicing, cảnh báo và câu chuyện kinh doanh.

## Slide 10 — Chạy local

Người chấm repo có thể chạy bằng Docker mà không cần tài khoản Azure. Dữ liệu mẫu dùng seed cố định để kết quả ổn định. MinIO đóng vai trò object storage tương thích S3. GitHub Actions chạy test tự động trên mỗi thay đổi. Đây là bằng chứng kỹ thuật mạnh hơn một ảnh dashboard không thể tái hiện.

## Slide 11 — Kết quả demo

Ở lần chạy demo bốn ngày: bảng Customer 360 có 500 dòng, Content KPI có 426 dòng. Ngày gần nhất có 252 watch raw, sau deduplicate còn 250, accepted 247 và quarantine 3. Search có 42 raw, 40 deduplicated, 37 accepted và 3 quarantined. Những con số này khớp đúng phương trình reconciliation.

## Slide 12 — Giá trị portfolio

Dự án thể hiện nhiều năng lực cùng lúc: modeling, partitioning, orchestration, idempotency, data quality, CI và giao tiếp kỹ thuật. Khi phỏng vấn, nên nhấn mạnh trade-off: ưu tiên tính đúng và khả năng tái hiện trên local, sau đó ánh xạ storage/compute/orchestration lên Azure thay vì tuyên bố đã triển khai cloud thật.

## Slide 13 — Kết

Thông điệp một câu: pipeline chỉ có giá trị khi dữ liệu đầu ra đáng tin cậy và người khác có thể kiểm chứng điều đó.
