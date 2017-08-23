import boto3

class GlacierCtx(object):
    """Context manager for glacier, sets defaults"""
    def __init__(self, ctx, region=None):
        self.region = region or ctx.config['glacier']['region']
        self.glacier = boto3.resource('glacier', region_name=self.region)

    def __enter__(self):
        return self.glacier

    def __exit__(self, *args):
        pass


class VaultCtx(GlacierCtx):
    """Context manager for glacier vault, sets defaults"""
    def __init__(self, ctx, vault_name=None, account_id=None, region=None):
        GlacierCtx.__init__(self, ctx, region)
        self.account_id = account_id or ctx.config['glacier']['account_id']
        self.vault_name = vault_name or ctx.config['glacier']['vault_name']
        self.vault = self.glacier.Vault(self.account_id, self.vault_name)

    def __enter__(self):
        return self.vault


class JobCtx(VaultCtx):
    """Context manager for glacier job, sets defaults"""
    def __init__(self, ctx, id, vault_name=None, account_id=None, region=None):
        VaultCtx.__init__(self, ctx, vault_name, account_id, region)
        self.job = self.glacier.Job(
            account_id=self.account_id,
            vault_name=self.vault_name,
            id=id
        )

    def __enter__(self):
        return self.job
