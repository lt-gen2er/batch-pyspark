import time
import requests

def slow_task_notification(threshold, webhook_url):
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            elapsed_time = end_time - start_time

            if elapsed_time > threshold:
                send_notification(webhook_url, "ETL Pipeline Task Slow Alert",
                                  f"Task took {elapsed_time:.2f} seconds to complete.")
            return result
        return wrapper
    return decorator

def send_notification(webhook_url, title, message):
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "0076D7",
        "summary": title,
        "sections": [
            {
                "activityTitle": title,
                "activitySubtitle": "ETL Pipeline Alert",
                "markdown": True,
                "text": message
            }
        ]
    }

    response = requests.post(webhook_url, json=payload)
    if response.status_code == 200:
        print("Notification sent successfully.")
    else:
        print("Failed to send notification.")
