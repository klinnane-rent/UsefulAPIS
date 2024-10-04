from constants.enums.foursquare_field_enum import FoursquareField
from model.address import Address


class FoursquarePlace:
    def __init__(self, foursquare_place_dict: dict):
        self.name = foursquare_place_dict.get(FoursquareField.NAME.value)
        self.location = foursquare_place_dict.get(FoursquareField.LOCATION.value)
        self.categories = foursquare_place_dict.get(FoursquareField.CATEGORIES.value)
        self.distance = foursquare_place_dict.get(FoursquareField.DISTANCE.value)
        self.email = foursquare_place_dict.get(FoursquareField.EMAIL.value)
        self.website = foursquare_place_dict.get(FoursquareField.WEBSITE.value)
        self.rating = foursquare_place_dict.get(FoursquareField.RATING.value)
        self.popularity = foursquare_place_dict.get(FoursquareField.POPULARITY.value)
        self.price = foursquare_place_dict.get(FoursquareField.PRICE.value)

        if self.location:
            address = Address(
                street_address=self.location.get("address"),
                address_extended=self.location.get("address_extended"),
                city=self.location.get("locality"),
                zip_code=self.location.get("postcode"),
                state=self.location.get("region"),
            )
            self.location = address
        if self.distance:
            self.distance = f"{int(self.distance) * 0.000621371:.2f} miles"  # meters to miles
        if self.popularity:
            self.popularity = f"{self.popularity:.4f}"

    def __str__(self):
        string = "Foursquare Place"
        variables = []
        for variable, value in self.__dict__.items():
            if value:
                variables.append(f"{variable}: {value}")
        return f"{string}: {', '.join(variables)}"

    def __repr__(self):
        return self.__str__()
