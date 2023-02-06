from setuptools import setup, find_packages

setup(
    name='andor-streamer',
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    packages=find_packages(),
    install_requires=['libdaq', 'pytango', 'pyzmq'],
    entry_points = {
        'console_scripts': ['Andor3 = andor_streamer.Andor3:main',]
    }
) 
 
