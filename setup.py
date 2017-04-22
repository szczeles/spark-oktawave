import setuptools
from spark_oktawave import __version__

setuptools.setup(
    name='spark-oktawave',
    version=__version__,
    url='https://github.com/szczeles/spark-oktawave',
    author='Mariusz Strzelecki',
    author_email='szczeles@gmail.com',
    license='Apache License 2.0',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Topic :: Utilities',
        'Environment :: Console',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],

    keywords='spark oktawave bigdata',

    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=[
        'click == 6.7',
        'zeep == 1.4.1'
    ],

    entry_points = {
        'console_scripts': ['spark-oktawave=spark_oktawave.spark_oktawave:main']
    }
)
