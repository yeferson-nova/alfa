from certification_orchestrator.application.ports import ConfigRepository
from certification_orchestrator.domain.configuration import read_global_config
from certification_orchestrator.domain.models import ConfigPointer, GlobalConfig


class GlobalConfigs:
    def __init__(self, repository: ConfigRepository) -> None:
        self._repository = repository
        self._loaded: dict[ConfigPointer, GlobalConfig] = {}

    def get(self, pointer: ConfigPointer) -> GlobalConfig:
        if pointer not in self._loaded:
            self._loaded[pointer] = read_global_config(self._repository.load_json(pointer.bucket, pointer.key))
        return self._loaded[pointer]
