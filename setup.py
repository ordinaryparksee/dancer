import setuptools

with open('README.md', 'r', encoding='utf-8') as fh:
    long_description = fh.read()

setuptools.setup(
    name='dancer',
    version='0.0.4',
    author='vaxy',
    author_email='ordinaryparksee@gmail.com',
    description='',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/ordinaryparksee/dancer.git',
    project_urls={
        'Bug Tracker': 'https://github.com/ordinaryparksee/dancer',
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Operating System :: OS Independent',
    ],
    package_dir={'': 'src'},
    packages=setuptools.find_packages(where='src'),
    python_requires='>=3.7',
)
