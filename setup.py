from setuptools import setup, find_packages

setup(
    name='cfkit',
    version='0.1.0',
    author='Noah',
    author_email='noah@chemforward.org',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'cachetools==5.5.0',
        'certifi==2024.7.4',
        'charset-normalizer==3.3.2',
        'et-xmlfile==1.1.0',
        'google-auth==2.34.0',
        'google-auth-oauthlib==1.2.1',
        'gspread==6.1.2',
        'gspread-dataframe==4.0.0',
        'gspread-formatting==1.2.0',
        'httplib2==0.22.0',
        'idna==3.8',
        'Levenshtein==0.25.1',
        'numpy==2.1.0',
        'oauth2client==4.1.3',
        'oauthlib==3.2.2',
        'openpyxl==3.1.5',
        'packaging==24.1',
        'pandas==2.2.2',
        'plotly==5.23.0',
        'psycopg2-binary==2.9.9',
        'pyasn1==0.6.0',
        'pyasn1_modules==0.4.0',
        'pyparsing==3.1.2',
        'python-dateutil==2.9.0.post0',
        'pytz==2024.1',
        'rapidfuzz==3.9.7',
        'requests==2.32.3',
        'requests-oauthlib==2.0.0',
        'rsa==4.9',
        'six==1.16.0',
        'SQLAlchemy==2.0.32',
        'tenacity==9.0.0',
        'tqdm==4.66.5',
        'typing_extensions==4.12.2',
        'tzdata==2024.1',
        'urllib3==2.2.2'
    ],
    entry_points={
        'console_scripts': [
            'cfkit=src.main:main',  # Adjust if necessary to match your project structure
        ],
    },
    python_requires='>=3.8',
)