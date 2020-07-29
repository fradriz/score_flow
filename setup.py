from setuptools import setup, find_packages


def readme():
    with open('README.md') as f:
        return f.read()


setup(name='score-flow-plus',
      version='1.4.1',
      description='COMPANY - AUTOMATATION of SCORE FLOW',
      url='https://',
      author='Facundo Radrizzani',
      author_email='fradriz@gmail.com',
      license='-',
      packages=find_packages(),
      install_requires=[
      ],
      test_suite='nose.collector',
      tests_require=['nose'],
      include_package_data=True,
      zip_safe=False)
