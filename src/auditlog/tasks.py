from celery import shared_task
import threading
from .receivers import log_create, log_update, log_delete


threadlocal = threading.local()


@shared_task
def log_create_task(sender, instance, created, **kwargs):
    remote_addr = threadlocal.auditlog['remote_addr']
    log_create(instance, created, remote_addr=remote_addr)


@shared_task
def log_update_task(*args, **kwargs):
    log_update(*args, **kwargs)


@shared_task
def log_delete_task(*args, **kwargs):
    log_delete(*args, **kwargs)


def _get_apply_async(task):
    def ret(*args, **kwargs):
        kwargs.pop('signal')  # Not pickleable, but not needed either.
        task.apply_async(args, kwargs,
                         serializer='pickle')  # Arguments come from Django signals, so we can use pickle securely here.

    return ret


log_create_async_receiver = _get_apply_async(log_create_task)
log_update_apply_async = _get_apply_async(log_update_task)
log_delete_apply_async = _get_apply_async(log_delete_task)
