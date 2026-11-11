from certification_orchestrator.domain.models import Action, StatusNames, StreamChange


def decide(change: StreamChange, statuses: StatusNames) -> Action:
    new_status = change.record.status
    if change.old_status == new_status:
        return Action.IGNORE
    actions = {
        statuses.pending: Action.LAUNCH_NOTIFICATION,
        statuses.certificate: Action.TRANSPORT,
        statuses.no_certificate: Action.ARCHIVE_REJECTED,
    }
    return actions.get(new_status, Action.IGNORE)
