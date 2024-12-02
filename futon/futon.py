import requests


class Database:
    def __init__(self, couchdb_url, username, password, ca_cert_path, db_name):
        self.couchdb_url = couchdb_url.rstrip("/")
        self.username = username
        self.password = password
        self.ca_cert_path = ca_cert_path
        self.db_name = db_name

    def create(self):
        """
        Create the database if it does not exist.

        Returns:
            bool: True if the database was created, False if it already existed.

        Raises:
            requests.exceptions.RequestException: For HTTP errors or connection issues.
        """
        if self.exists():
            return False  # Database already exists

        db_url = f"{self.couchdb_url}/{self.db_name}"
        auth = (self.username, self.password) if self.username and self.password else None

        try:
            response = requests.put(db_url, auth=auth, verify=self.ca_cert_path)
            response.raise_for_status()
            print(f"Database '{self.db_name}' created successfully.")
            return True
        except requests.exceptions.RequestException as e:
            print(f"Error creating database '{self.db_name}': {e}")
            raise

    def exists(self):
        """
        Check if the database exists.

        Returns:
            bool: True if the database exists, False otherwise.

        Raises:
            requests.exceptions.RequestException: For HTTP errors or connection issues.
        """
        db_url = f"{self.couchdb_url}/{self.db_name}"
        auth = (self.username, self.password) if self.username and self.password else None

        try:
            response = requests.head(db_url, auth=auth, verify=self.ca_cert_path)
            if response.status_code == 200:
                return True  # Database exists
            elif response.status_code == 404:
                return False  # Database does not exist
            else:
                response.raise_for_status()  # Raise an exception for unexpected status codes
        except requests.exceptions.RequestException as e:
            print(f"Error checking existence of database '{self.db_name}': {e}")
            raise

    def find(self, selector=None, columns=None, sort=None, descending=False, limit=None):
        """
        Fetch data from a CouchDB database with optional Mango query parameters.

        Args:
            selector (dict, optional): A Mango query selector to filter the documents. Defaults to {}.
            columns (list, optional): A list of fields to include in the result. Defaults to None (all fields).
            sort (str or list, optional): A field name (str) or Mango-style list of sort conditions. Defaults to None.
            descending (bool, optional): If True, sorts in descending order. Defaults to False.
            limit (int, optional): Limits the number of results returned. Defaults to None (no limit).

        Returns:
            list: Documents matching the query.

        Raises:
            requests.exceptions.RequestException: For HTTP errors or connection issues.

        """

        db_url = f"{self.couchdb_url}/{self.db_name}/_find"
        auth = (self.username, self.password) if self.username and self.password else None
        selector = selector if selector else {}
        payload = {
            "selector": selector
        }

        if columns:
            payload["fields"] = columns

        # Handle sort argument
        if sort:
            if isinstance(sort, str):
                payload["sort"] = [{sort: "desc" if descending else "asc"}]
            elif isinstance(sort, list):
                payload["sort"] = sort

        if limit:
            payload["limit"] = limit

        try:
            response = requests.post(db_url, json=payload, auth=auth, verify=self.ca_cert_path)
            response.raise_for_status()
            data = response.json()
            return data.get("docs", [])

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from CouchDB: {e}")
            raise

    def insert_many(self, records):
        """
        Insert multiple records into a CouchDB database.

        Args:
            records (list): A list of dictionaries, where each dictionary represents a document to insert.

        Returns:
            list: A list of responses from the CouchDB server for each inserted record.

        Raises:
            requests.exceptions.RequestException: For HTTP errors or connection issues.

        """

        db_url = f"{self.couchdb_url}/{self.db_name}/_bulk_docs"
        auth = (self.username, self.password) if self.username and self.password else None
        payload = {"docs": records}

        try:
            response = requests.post(db_url, json=payload, auth=auth, verify=self.ca_cert_path)
            response.raise_for_status()
            return response.json()  # Parse and return the JSON response
        except requests.exceptions.RequestException as e:
            print(f"Error inserting records into CouchDB: {e}")
            raise

    def insert_one(self, document):
        """
        Insert a single document into a CouchDB database.

        Args:
            document (dict): The document to insert.

        Returns:
            dict: The CouchDB response for the insertion.

        Raises:
            requests.exceptions.RequestException: For HTTP errors or connection issues.

        """

        db_url = f"{self.couchdb_url}/{self.db_name}"
        auth = (self.username, self.password) if self.username and self.password else None

        try:
            response = requests.post(db_url, json=document, auth=auth, verify=self.ca_cert_path)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error inserting document into CouchDB: {e}")
            raise


class Client:
    """
    A client interface for interacting with a CouchDB server.
    """

    def __init__(self, couchdb_url, username=None, password=None, ca_cert_path=None):
        """
        Initialize the CouchDB client.

        Args:
            couchdb_url (str): The URL of the CouchDB instance (e.g., 'https://192.168.1.11:6984').
            username (str, optional): Username for CouchDB authentication. Default is None.
            password (str, optional): Password for CouchDB authentication. Default is None.
            ca_cert_path (str, optional): Path to a CA certificate file for SSL verification. Default is None.

        """

        self.couchdb_url = couchdb_url.rstrip("/")
        self.username = username
        self.password = password
        self.ca_cert_path = ca_cert_path
        self._databases = None

    def __getitem__(self, db_name):
        """
        Return a Database object for the specified database.
        Create the database if it does not exist.

        Args:
            db_name (str): The name of the database.

        Returns:
            Database: An instance of the Database class.
        """

        return Database(self.couchdb_url, self.username, self.password, self.ca_cert_path, db_name)

    @property
    def databases(self):
        """
        A list of all databases in CouchDB that this account has access to.

        """

        if self._databases:
            return self._databases

        db_url = f"{self.couchdb_url}/_all_dbs"
        auth = (self.username, self.password) if self.username and self.password else None
        try:
            response = requests.get(db_url, auth=auth, verify=self.ca_cert_path)
            response.raise_for_status()
            self._databases = response.json()
            return self._databases

        except requests.exceptions.RequestException as e:
            print(f"Error fetching databases: {e}")
            raise
