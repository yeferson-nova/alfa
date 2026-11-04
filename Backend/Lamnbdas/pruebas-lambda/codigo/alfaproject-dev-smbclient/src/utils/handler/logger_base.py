import logging

console_handler = logging.StreamHandler()
console_format = logging.Formatter(
    fmt="%(asctime)s - %(levelname)s\t[%(filename)s: %(lineno)s] %(message)s",
    datefmt="%Y-%m-%d - %I:%M:%S %p",
)
console_handler.setFormatter(console_format)

log = logging.getLogger("logger_name")
log.setLevel(logging.INFO)
log.addHandler(console_handler)