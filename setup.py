from setuptools import setup


setup(
    name="asyncbolt",
    version="0.0.2",
    license="MIT",
    author="David M. Brown",
    author_email="davebshow@gmail.com",
    description="Bolt client/server protocol for Python asyncio",
    url='https://github.com/davebshow/asyncbolt',
    packages=['asyncbolt'],
    extras_require={
        'test': ['pytest', 'pytest-asyncio']},
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6'
    ]
)