class Address:
    def __init__(self, street_address: str, city: str, state: str, zip_code: str, address_extended: str = None):
        self.street_address = street_address
        self.address_extended = address_extended
        self.city = city
        self.state = state
        self.zip_code = zip_code

    def __str__(self):
        if self.address_extended:
            address = f"{self.street_address} {self.address_extended}"
        else:
            address = self.street_address
        return f"{address}, {self.city}, {self.state} {self.zip_code}"

    def __hash__(self):
        return hash((self.street_address, self.address_extended, self.city, self.state, self.zip_code))
