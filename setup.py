__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENCE file in top-level package directory"

from setuptools import setup, find_packages

from gws_migration_tools import __version__ as _package_version

setup(
    name='gws_migration_tools',
    version=_package_version,
    description='GWS migration tools',
    url='https://github.com/cedadev/gws_migration_tools/',
    license='BSD - See gws_migration_tools/LICENCE file for details',
    packages=find_packages(),
    package_data={
        'gws_migration_tools': [
            'LICENCE',
        ],
    },
    install_requires=[
        # 'requests',
    ],
    
    # This qualifier can be used to selectively exclude Python versions - 
    # in this case early Python 2 and 3 releases
    python_requires='>=3.5.0', 
    
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'request-migration = gws_migration_tools.request_cli:main_migration',
            'request-retrieval = gws_migration_tools.request_cli:main_retrieval',
            'request-offline-copy-deletion = gws_migration_tools.request_cli:main_deletion',
            'list-offline-requests = gws_migration_tools.request_cli:main_list',
            'withdraw-offline-request = gws_migration_tools.request_cli:main_withdraw',

            'init-migrations = gws_migration_tools.init_migrations:main',
            'handle-offline-requests = gws_migration_tools.handle_requests:main',
            ],
        }
)
