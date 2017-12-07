from setuptools import setup, find_packages


setup(
    name="asyncbolt",
    version="0.0.1",
    license="MIT",
    author="davebshow",
    author_email="davebshow@gmail.com",
    description="Bolt protocol for Asyncio",
    packages=find_packages(exclude=['tests']),
    extras_require={
        'test': ['pytest', 'pytest-asyncio']},
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6'
    ]
)