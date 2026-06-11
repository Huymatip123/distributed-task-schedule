# Distributed Task Scheduler — Hướng dẫn chạy trên Ubuntu

Đây là bản cài đặt mẫu (Python 3, dùng TCP socket + JSON) cho đề bài
**"Task Scheduling in Distributed Operating Systems"**. Mỗi phần trong code
đều được chú thích để bạn thấy nó tương ứng với mục nào trong đề.

## 1. Cấu trúc thư mục

```
scheduler/
├── net.py                 # Lớp MessageStream: gói/nhận message JSON qua TCP (Mục 4)
├── tasks.py                # 4 loại task CPU-bound (Mục 5: A,B,C,D)
├── master.py                # Master Node (Mục 2,3.3,6,7,8,9)
├── worker.py                 # Worker Node (Mục 3.1,5,6,9)
├── client.py                  # Gửi task / xem thống kê (Mục 3.2)
└── experiments/
    ├── run_experiment.py        # Chạy 1 thí nghiệm (master+worker tự động)
    ├── plot_results.py            # Thí nghiệm 1 & 2 + vẽ biểu đồ
    └── exp3_failure.py             # Thí nghiệm 3: failure recovery
```

## 2. Chuẩn bị môi trường (Ubuntu)

Chỉ cần Python 3 (có sẵn trên Ubuntu 22.04/24.04):

```bash
python3 --version          # >= 3.8 là chạy được
sudo apt update
sudo apt install -y python3 python3-pip
pip3 install matplotlib --break-system-packages   # chỉ cần cho vẽ biểu đồ (Thí nghiệm 1,2)
```

Giải nén project, ví dụ vào `~/scheduler`, rồi `cd ~/scheduler`.

## 3. Chạy thủ công (để hiểu kiến trúc — Mục 2,3,9)

Mở **nhiều terminal** (mô phỏng nhiều "máy"):

**Terminal 1 — Master** (chọn 1 trong 3 chính sách lập lịch ở Mục 3.3):
```bash
python3 master.py --worker-port 6000 --client-port 6001 --policy fifo
# policy có thể là: fifo | round_robin | least_loaded
```
Master in ra log: worker đăng ký, gán task, nhận kết quả, phát hiện lỗi...

**Terminal 2,3,4 — Worker 1,2,3** (mỗi worker là 1 "máy" riêng, Mục 3.1):
```bash
python3 worker.py --master-port 6000 --worker-id 1 --cpu-cores 2
python3 worker.py --master-port 6000 --worker-id 2 --cpu-cores 2
python3 worker.py --master-port 6000 --worker-id 3 --cpu-cores 2
```
- `--cpu-cores N` = số task worker này chạy song song (Mục 3.1: cpu_cores=4).
- Mỗi worker tự gửi `HEARTBEAT` mỗi 2 giây (Mục 6).

**Terminal 5 — Client gửi task (Mục 3.2)**:
```bash
# Task A: Prime Counting
python3 client.py --port 6001 --num-tasks 10 --type prime_count --input 1000000

# Task B: Matrix Multiplication 200x200
python3 client.py --port 6001 --num-tasks 5 --type matrix_mult --input 200

# Task C: Monte Carlo Pi với 10 triệu mẫu
python3 client.py --port 6001 --num-tasks 5 --type monte_carlo_pi --input 10000000

# Task D: Word Count
python3 client.py --port 6001 --num-tasks 5 --type word_count --input 50000

# Xem thống kê
python3 client.py --port 6001 --stats
```

Quan sát log ở Terminal 1 (Master) và các Terminal Worker để thấy:
- Master gán task theo chính sách đã chọn (`Task X -> Worker Y`).
- Worker in ra "executing task X".
- Master in "Task X COMPLETED ...".

## 4. Ánh xạ code <-> đề bài

| Mục trong đề | Nơi cài đặt |
|---|---|
| 3.1 Worker Registration | `worker.py` gửi `REGISTER`; `master.py: handle_worker()` lưu vào `self.workers` (WorkerTable) |
| 3.2 Task Submission | `client.py: submit_tasks()`; `master.py: handle_client()` tạo `Task{task_id, type, input, status,...}` |
| 3.3 FIFO / Round Robin / Least Loaded | `master.py: scheduler_loop()` |
| 4. Communication Protocol (JSON) | `net.py: MessageStream` (REGISTER, TASK, RESULT, HEARTBEAT, ACK, STATS) |
| 5. Task Types A-D | `tasks.py: prime_count, matrix_mult, monte_carlo_pi, word_count` |
| 6. Heartbeat / Failure Detection | `worker.py: heartbeat_loop()`; `master.py: heartbeat_monitor()` (timeout 6s) |
| 7. Failure Recovery (RUNNING->READY) | `master.py: mark_failed()` |
| 8. struct WorkerInfo / Task | `master.py: self.workers[...]` và `self.tasks[...]` (dict tương đương struct) |
| 9. Thread Design | Master: `accept_workers`, `accept_clients`, `scheduler_loop`, `heartbeat_monitor`. Worker: `heartbeat_loop`, `worker_thread` (x cpu_cores). Đồng bộ bằng `threading.Lock` |

## 5. Thí nghiệm tự động (Mục 10)

### Thí nghiệm 1 — Scalability (100 task, 1/2/4/8 worker)
### Thí nghiệm 2 — So sánh FIFO / Round Robin / Least Loaded
Chạy cả hai cùng lúc và vẽ biểu đồ:

```bash
cd experiments
python3 plot_results.py
```

Kết quả:
- `exp1_scalability.png` — trục X: số worker, 2 trục Y: thời gian hoàn thành & throughput.
  Theo lý thuyết, càng nhiều worker thì thời gian hoàn thành càng giảm
  (throughput tăng) — đúng như kỳ vọng trong đề ("More workers → faster execution").
- `exp2_policies.png` — trục X: số worker, trục Y: average response time,
  mỗi đường ứng với 1 chính sách lập lịch (đúng như "Plot: Workers vs Response Time").

Bạn cũng có thể chạy một cấu hình đơn lẻ:
```bash
python3 run_experiment.py --workers 4 --tasks 100 --policy round_robin \
    --type prime_count --input 200000
```
In ra: `completion_time`, `throughput`, `avg_response_time`.

### Thí nghiệm 3 — Failure Recovery (kill -9 worker giữa chừng)

```bash
cd experiments
python3 exp3_failure.py
```

Script này:
1. Khởi động Master (policy `least_loaded`) + 3 Worker.
2. Gửi 20 task tính số nguyên tố (mỗi task ~1.5-2 giây để có "thời gian" giữa
   chừng).
3. Sau ~0.5s, **`kill -9` Worker 2** — đúng như đề yêu cầu mô phỏng crash.
4. In trạng thái mỗi giây: `ready / running / completed` và bảng worker
   (`alive`, `current_load`).
5. Khẳng định: tất cả 20 task đều hoàn thành (`COMPLETED == 20`) dù Worker 2
   đã chết — tức **không mất task nào** (đúng yêu cầu "Verify: No task lost").

Bạn sẽ thấy log kiểu:
```
[MASTER] Task 2 requeued (worker 2 failed)
...
SUCCESS: all 20 tasks completed despite worker 2 failing. No task lost.
```

### Tự kiểm tra thủ công kịch bản kill -9

Nếu muốn tự tay làm đúng như mô tả "kill -9 worker2" trong đề (3 terminal
worker + 1 terminal master), chạy như Mục 3, gửi một loạt task lớn
(vd `--type prime_count --input 100000000 --num-tasks 20`), rồi ở terminal
Worker 2 nhấn `Ctrl+\` (SIGQUIT) hoặc tìm PID và `kill -9 <pid>`:

```bash
ps aux | grep "worker.py --master-port 6000 --worker-id 2"
kill -9 <PID>
```

Theo dõi log Master: sau tối đa ~6 giây (HEARTBEAT_TIMEOUT) bạn sẽ thấy:
```
[MASTER] Worker 2 FAILED (heartbeat timeout > 6s)
[MASTER] Task 22 requeued (worker 2 failed)
[MASTER] Task 22 (...) -> Worker <khác>
```

## 6. Đồng bộ hoá (synchronization) — giải thích cho báo cáo

- **Master** dùng **một `threading.Lock` duy nhất** (`self.lock`) bảo vệ 3
  cấu trúc dữ liệu chung: `self.workers` (WorkerTable), `self.tasks`
  (Task table), `self.queue` (hàng đợi READY). Mọi thread (accept_workers,
  accept_clients, scheduler_loop, heartbeat_monitor) đều `with self.lock:`
  trước khi đọc/ghi — tương đương "mutex" được đề cập ở Mục 9.
- **Worker** dùng `queue.Queue` (an toàn đa luồng sẵn) để chuyển task từ
  thread nhận mạng sang các thread thực thi (`cpu_cores` threads), và một
  `threading.Lock` (`send_lock`) để heartbeat-thread và task-thread không
  ghi đè lên nhau khi gửi qua cùng 1 socket.

## 7. Mở rộng / nâng cấp (tuỳ chọn)

- Đổi `prime_count`/`matrix_mult`/... sang đa tiến trình (`multiprocessing`)
  nếu muốn tận dụng nhiều CPU thật (hiện tại dùng `threading`, bị giới hạn
  bởi GIL của Python cho code thuần Python — tuy nhiên với mục đích minh hoạ
  scheduling/hearbeat/failure thì vẫn đủ).
- Có thể viết lại bằng C/C++ với pthread + TCP socket, hoặc dùng gRPC/ZeroMQ
  như đề gợi ý — giao thức JSON ở `net.py` có thể giữ nguyên ý tưởng
  (REGISTER/TASK/RESULT/HEARTBEAT).
- Thêm cơ chế lưu log ra file để vẽ biểu đồ chi tiết hơn (Gantt chart task
  theo thời gian, theo từng worker).

## 8. Khắc phục sự cố thường gặp

- `Address already in use`: cổng đang bị chiếm (chạy lần trước chưa tắt) —
  đổi `--worker-port`/`--client-port` hoặc `pkill -f master.py`.
- Muốn dừng tất cả: `pkill -f master.py; pkill -f worker.py`.
- `pip3 install matplotlib` báo lỗi "externally-managed-environment": thêm
  `--break-system-packages` như hướng dẫn ở Mục 2.
