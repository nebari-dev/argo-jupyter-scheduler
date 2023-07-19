from jupyter_scheduler.task_runner import (
    JobDefinitionTask,
    TaskRunner,
    UpdateJobDefinitionCache,
)

from argo_jupyter_scheduler.utils import setup_logger

logger = setup_logger(__name__)


class ArgoTaskRunner(TaskRunner):
    def process_queue(self):
        logger.debug("Start process_queue...")
        self.log.debug(self.queue)
        while not self.queue.isempty():
            logger.debug("Processing queue.")
            task = self.queue.peek()
            cache = self.cache.get(task.job_definition_id)

            if not cache:
                self.queue.pop()
                continue

            cache_run_time = cache.next_run_time
            queue_run_time = task.next_run_time

            if not cache.active or queue_run_time != cache_run_time:
                self.queue.pop()
                continue

            time_diff = self.compute_time_diff(queue_run_time, cache.timezone)

            # if run time is in future
            if time_diff < 0:
                break
            else:
                try:
                    # TODO: check that the Argo CronWorkflow is still running, if not delete record
                    pass
                except Exception as e:
                    self.log.exception(e)
                self.queue.pop()
                run_time = self.compute_next_run_time(cache.schedule, cache.timezone)
                self.cache.update(
                    task.job_definition_id,
                    UpdateJobDefinitionCache(next_run_time=run_time),
                )
                self.queue.push(
                    JobDefinitionTask(
                        job_definition_id=task.job_definition_id, next_run_time=run_time
                    )
                )
