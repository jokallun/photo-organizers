from invoke import Program, Collection, Config
import archive_to_glacier
import glacier_inventory

ns = Collection()
ns.add_collection(glacier_inventory, 'inventory')
ns.add_collection(archive_to_glacier, 'glacier')

class PhotoConfig(Config):
    file_prefix = 'photo_organizer'

program = Program(
    namespace=ns,
    version='0.1.0',
    config_class=PhotoConfig
)
