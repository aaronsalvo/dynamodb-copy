from setuptools import setup

setup(
    name='dynamodb-copy',
    version='1.0',
    packages=['cli'],
    url='',
    license='MIT',
    author='Aaron Salvo',
    author_email='aaron.salvo@callibrity.com',
    description='A simple utility to copy data from one DynamoDB table to another with the same structure',
    project_urls={
        'Source': 'https://github.com/callibrity/dynamodb-copy'
    },
    entry_points={
        'console_scripts': [
            'ddbcp=cli:run',
        ],
    },
)
