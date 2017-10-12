from setuptools import setup, find_packages


def parse_requirements():
    requirements = []
    for reqf in ('requirements.txt', 'requirements-swh.txt'):
        with open(reqf) as f:
            for line in f.readlines():
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                requirements.append(line)
    return requirements


# Edit this part to match your module
# full sample: https://forge.softwareheritage.org/diffusion/DCORE/browse/master/setup.py
setup(
    name='swh.objstorage',
    description='Software Heritage Object Storage',
    author='Software Heritage developers',
    author_email='swh-devel@inria.fr',
    url='https://forge.softwareheritage.org/diffusion/DOBJS',
    packages=find_packages(),
    scripts=[
        'bin/swh-objstorage-add-dir',
        'bin/swh-objstorage-fsck'
    ],   # scripts to package
    install_requires=parse_requirements(),
    setup_requires=['vcversioner'],
    vcversioner={},
    include_package_data=True,
)
