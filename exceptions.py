class ExecutionException(Exception):
    def __init__(self, task, exception):
        super().__init__()
        self.task = task
        self.exception = exception