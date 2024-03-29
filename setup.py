from setuptools import setup, find_packages

setup(
    name='kombu-redis-priority',
    packages=find_packages(),
    version='1.1.0',
    description='Celery backend using redis SortedSets for priority',
    include_package_data=True,
    author='Captricity',
    author_email='webmaster@captricity.com',
    url='https://github.com/Captricity/kombu-redis-priority',
    download_url='https://github.com/Captricity/kombu-redis-priority/tarball/1.1.0',
    keywords=['redis', 'sorted-set', 'kombu'],
    classifiers=[],
    install_requires=[
        'kombu',
        'importlib-metadata==4.13.0',
    ],
    tests_require=[
        'freezegun',
        'fakeredis',
        'ddt'
    ],
    test_suite='tests'
)
