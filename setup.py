from setuptools import setup, find_packages

setup(
    name='dev-andor3',
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    packages=find_packages(),
    install_requires=['libdaq', 'pytango', 'pyzmq'],
    entry_points = {
        'console_scripts': ['Andor3 = dev_andor3.Andor3:main',]
    }
) 
 
