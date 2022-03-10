# todo: implement
class BackupConfiguration:
    pass


class PeriodicBackupConfiguration(BackupConfiguration):
    pass


class BackupSettings:
    pass


class LocalSettings(BackupSettings):
    pass


class AmazonSettings(BackupSettings):
    pass


class S3Settings(AmazonSettings):
    pass


class AzureSettings(BackupSettings):
    pass


class GlacierSettings(AmazonSettings):
    pass


class BackupStatus:
    pass


class GoogleCloudSettings(BackupStatus):
    pass


class FtpSettings(BackupSettings):
    pass
