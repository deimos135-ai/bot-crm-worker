from datetime import timedelta, timezone
KYIV_TZ = timezone(timedelta(hours=3))  # літній/зимовий можна доручити dateutil, але для MVP так ок
