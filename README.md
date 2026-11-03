# Olympia Live – trò chơi ô chữ 6 đội

## Cài đặt
Cần Node.js 18 trở lên.

```bash
npm install
npm start
```

Trên máy giáo viên mở http://localhost:3000, chọn **Giáo viên – Tạo phòng**.

Để học sinh truy cập qua Wi-Fi cùng mạng, tìm IP nội bộ của máy giáo viên, rồi mở `http://IP-CUA-MAY-GIAO-VIEN:3000` trên điện thoại học sinh. Đảm bảo tường lửa cho phép cổng 3000.

Để sử dụng qua Internet khác mạng, triển khai ứng dụng Node.js lên một máy chủ hỗ trợ WebSocket (ví dụ Render/Railway) và sử dụng đường dẫn HTTPS do dịch vụ cung cấp.

## Lưu ý
- Giáo viên phải giữ tab mở; phiên chơi chỉ tồn tại trong bộ nhớ máy chủ.
- Đáp án chỉ được gửi đến giao diện giáo viên; các đội không xem được đáp án đội khác.
- Mỗi đội chỉ gửi một đáp án cho mỗi câu hỏi; giáo viên mở hàng tiếp theo để đặt lại.
- Đây là phiên bản mẫu. Mã phòng không thay thế cơ chế đăng nhập/xác thực bảo mật của bản triển khai công khai.
- Nếu tải lại trang giáo viên, phòng hiện tại sẽ đóng; cần tạo phòng mới.
