from setuptools import setup, find_packages

setup(
    name='kombu-redis-priority',
    packages=find_packages(),
    version='0.1.0',
    description='Celery backend using redis SortedSets for priority',
    include_package_data=True,
    author='Captricity',
    author_email='webmaster@captricity.com',
    url='https://github.com/Captricity/kombu-redis-priority',
    download_url='https://github.com/Captricity/kombu-redis-priority/tarball/0.1.0',
    keywords=['redis', 'sorted-set', 'kombu'],
    classifiers=[],
    install_requires=[
        'kombu'
    ],
    tests_require=[
        'six',
        'mock==1.0.1',
        'freezegun',
        'fakeredis'
    ],
    test_suite='tests'
)
