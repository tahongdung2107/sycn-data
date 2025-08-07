# Hàm Xóa Dữ Liệu Bảng Cha và Bảng Con

## Mô tả

Hàm `delete_records_with_children_by_date` được tạo để giải quyết vấn đề xóa dữ liệu ở các bảng con (child tables) khi xóa dữ liệu ở bảng cha (parent table).

## Vấn đề ban đầu

Khi sử dụng hàm `delete_records_by_date` cũ, chỉ có dữ liệu ở bảng cha được xóa, trong khi dữ liệu ở các bảng con có foreign key (`fk_id`) vẫn còn tồn tại, gây ra:

- Dữ liệu orphan (dữ liệu con không có cha)
- Không nhất quán trong database
- Có thể gây lỗi khi query dữ liệu

## Giải pháp

Hàm `delete_records_with_children_by_date` thực hiện xóa dữ liệu theo thứ tự:

1. **Bước 1**: Lấy danh sách ID của các record cần xóa từ bảng cha
2. **Bước 2**: Xóa dữ liệu trong các bảng con trước (dựa trên `fk_id`)
3. **Bước 3**: Xóa dữ liệu trong bảng cha

## Các bảng con được hỗ trợ

### Orders
- `orders_tags` - Tags của đơn hàng
- `orders_products` - Sản phẩm trong đơn hàng
- `orders_packed` - Thông tin đóng gói
- `orders_facebook` - Thông tin Facebook
- `orders_vat` - Thông tin VAT

### Bills
- `bills_tags` - Tags của hóa đơn
- `bills_products` - Sản phẩm trong hóa đơn

## Cách sử dụng

```python
from service.insertUpdateData import delete_records_with_children_by_date

# Xóa orders và bảng con (sử dụng timestamp)
delete_records_with_children_by_date('orders', 'updatedAt', start_timestamp, end_timestamp)

# Xóa bills và bảng con (sử dụng date string)
delete_records_with_children_by_date('bills', 'createdDateTime', '2024-01-01', '2024-02-01')
```

## Tham số

- `table_name`: Tên bảng cha ('orders' hoặc 'bills')
- `date_field`: Tên trường ngày tháng ('updatedAt', 'createdDateTime', ...)
- `start_date`: Ngày bắt đầu (có thể là timestamp hoặc string)
- `end_date`: Ngày kết thúc (có thể là timestamp hoặc string)

## Lưu ý về định dạng thời gian

- **Orders**: Sử dụng timestamp (ví dụ: 1704152526)
- **Bills**: Sử dụng date string (ví dụ: '2024-01-01 11:25:09')

## Cập nhật trong code

Các file đã được cập nhật:

1. `service/insertUpdateData.py`: Thêm hàm `delete_records_with_children_by_date`
2. `main.py`: Sử dụng hàm mới thay vì `delete_records_by_date`
3. `main_1.py`: Sử dụng hàm mới thay vì `delete_records_by_date`

## Kết quả test

Hàm đã được test thành công với dữ liệu thực tế:

- **Orders**: Xóa 26 record cha + 110 record con
- **Bills**: Xóa 1163 record cha + 2903 record con

## Lợi ích

1. **Tính nhất quán**: Đảm bảo không có dữ liệu orphan
2. **Hiệu suất**: Xóa theo batch, tối ưu cho database lớn
3. **An toàn**: Sử dụng transaction để rollback nếu có lỗi
4. **Linh hoạt**: Hỗ trợ cả timestamp và date string 