
class NumberUtils:
    @staticmethod
    def nullOrDefault(number, default):
        if number is None:
            return default
        else:
            return number