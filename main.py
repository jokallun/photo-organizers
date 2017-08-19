from invoke import Program, Collection, Config
import tasks

class PhotoConfig(Config):
    file_prefix = 'photo_organizer'

program = Program(
    namespace=Collection.from_module(tasks),
    version='0.1.0',
    config_class=PhotoConfig
)
