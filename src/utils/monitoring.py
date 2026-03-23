import time
import functools
from src.utils.logger import get_logger

logger = get_logger("monitoring")

metrics = {}

def track_time(step_name):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            logger.info(f"[START] {step_name}")
            try:
                result = func(*args, **kwargs)
                duration = round(time.time() - start, 2)
                metrics[step_name] = {"status": "✅ success", "duration_s": duration}
                logger.info(f"[END] {step_name} — {duration}s")
                return result
            except Exception as e:
                duration = round(time.time() - start, 2)
                metrics[step_name] = {"status": "❌ failed", "duration_s": duration, "error": str(e)}
                logger.error(f"[FAIL] {step_name} — {duration}s — {e}")
                raise
        return wrapper
    return decorator

def print_report():
    print("\n" + "="*55)
    print("📊  PIPELINE PERFORMANCE REPORT")
    print("="*55)
    total = 0
    for step, data in metrics.items():
        total += data["duration_s"]
        print(f"  {data['status']}  {step:<30} {data['duration_s']}s")
    print("-"*55)
    print(f"  {'TOTAL':<38} {round(total, 2)}s")
    print("="*55 + "\n")
