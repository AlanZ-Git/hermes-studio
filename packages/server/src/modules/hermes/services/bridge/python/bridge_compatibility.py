import os


def protected_rollback():
    return os.environ.get('HERMES_STUDIO_COMPATIBILITY_ROLLBACK', '').lower() in {'1', 'true', 'on'}


def guard(request):
    if not protected_rollback():
        return
    action = request.get('action')
    if (action in {'chat', 'context_estimate', 'background_poll', 'background_notification_complete',
                   'background_notification_claim', 'loop_command', 'loop_recover', 'command',
                   'goal_evaluate', 'notification_retention_reconcile'}
            or action == 'loop_operation' and request.get('op') not in {'list', 'recovery_status'}):
        raise RuntimeError('Compatibility rollback: scheduling, notification consumption and model entry points paused')
