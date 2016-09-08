import asyncio
import logging

import github


class PullRequest:
    def __init__(self, payload, auth_token):
        self.gh_obj = github.Github(login_or_token=auth_token)
        self.repo_obj = self.gh_obj.get_repo(payload['repository']['id'])
        self.pr_obj = self.repo_obj.get_pull(payload['number'])

    @asyncio.coroutine
    def update_status(self, state: str, target_url: str, message_id, description: str=None):
        logger = logging.getLogger(__name__)
        logger.info('Setting status on pull request #{0} for {1} to "{2}"'.format(
            self.pr_obj.number,
            self.repo_obj.full_name,
            state
        ), extra={'MESSAGE_ID': message_id})
        latest_commit = self.pr_obj.get_commits().reversed[0]
        logger.debug('Using commit {} to set status'.format(latest_commit.sha), extra={'MESSAGE_ID': message_id})
        latest_commit.create_status(state=state, description=description, target_url=target_url, context='pcts')

    def get_files(self):
        return [file.filename for file in self.pr_obj.get_files()]

    @property
    def number(self):
        return self.pr_obj.number

    @property
    def repo(self):
        return self.repo_obj.ssh_url

    @property
    def base_ref(self):
        return self.pr_obj.base.ref
