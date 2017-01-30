from setuptools import setup, find_packages

setup(
    name='kombu-redis-priority',
    packages=find_packages(),
    version='0.0.6',
    description='Celery backend using redis SortedSets for priority',
    include_package_data=True,
    author='Captricity',
    author_email='webmaster@captricity.com',
    url='https://github.com/Captricity/kombu-redis-priority',
    download_url='https://github.com/Captricity/kombu-redis-priority/tarball/0.0.5',
    keywords=['redis', 'sorted-set', 'kombu'],
    classifiers=[],
    install_requires=[
        'kombu'
    ]
)
