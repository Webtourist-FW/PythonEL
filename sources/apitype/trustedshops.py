""" Definiert den Umgang mit der Trusted-Shops-API.

    Classes:
        TrustedShopsSource
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
import logging as log
from typing import Generator

# 3rd-Party Importe
import pandas as pd
import httpx

# Eigene Module
from braxel.util.data_package import DataPackage
from braxel.sources.apitype.base import APISource

# Set up Logger
log.getLogger(__name__).addHandler(log.NullHandler())


# FUNKTIONEN / KLASSEN

class TrustedShopsSource(APISource):
    def __init__(
            self,
            name: str,
            type: str,
            baseurl: str,
            path: str = '',
            urlvars: dict | None = None,
            parameters: dict | None = None) -> None:
        super().__init__(name, type, baseurl, path, urlvars, parameters)

    def check(self) -> bool:
        """Prüft die Verbindung zur TrustedShops-Quelle.

        Returns:
            bool: True, wenn eine Response mit Status 200 zurückkommt.
        """
        response = httpx.get(self.url(), params=self.parameters)
        return response.status_code == 200

    # TODO: Alte Logik. Muss überarbeitet und vereinfacht werden.
    def _get_empty_reviews_df(self):
        """Creates an empty dataframe with all the necessary columns.

        Returns:
            pd.DataFrame -- Empty dataframe with all required trusted shop columns.
        """
        reviews_columns = [
            'reviewID',
            'shopID',
            'url',
            'productSKU',
            'productID',
            'fabricID',
            'modelID',
            'colorID',
            'size',
            'styleName',
            'overallMark',
            'overallRating',
            'reviewDate',
            'orderDate',
            'reviewer',
            'reviewerLocation',
            'reviewMark',
            'reviewComment']
        df = pd.DataFrame(columns=reviews_columns)
        return df

    # TODO: Falls notwendig, besser in braxel.util auslagern
    def _reformat_size(self, size):
        """Adds leading zeros to sizes with less than 3 digits-

        Arguments:
            size {int or string} -- size

        Returns:
            str -- Size including leading zeros.
        """
        size = str(size)
        if size.isnumeric() and len(size) == 2:
            size = '0' + size
        return size

    # TODO: Falls notwendig, evtl. in APISource oder braxel.util auslagern
    def _parse_url(self, url):
        """Parses URL and returns the route as well as the parameters as a dictionary.

        Arguments:
            url {string} -- URL to be parsed

        Returns:
            tuple -- first argument is the route, second is a dictionary with all parameters
        """
        url = url.replace('&amp;', '&')
        url = url.replace('%2f', '/')
        url = url.split('?')
        route = url[0]
        parameters_list = url[1].split('&')
        parameters = dict()

        for parameter in parameters_list:
            parameter_name, parameter_value = parameter.split('=')
            parameters[parameter_name] = parameter_value

        return route, parameters

    # TODO: Alte Logik. Muss überarbeitet und vereinfacht werden.
    def _process_shop_data(self, shop_data):
        """Takes JSON dictionary with shop data, flattens it and returns it as a dataframe.

        Arguments:
            shop_data {dict} -- JSON-dict to be flattened and transformed into a dataframe.

        Returns:
            pd.DataFrame -- Dataframe containing the given shop data.
        """
        df = self._get_empty_reviews_df()
        # create a copy of the df for repeated use, that doesn't change the
        # original
        shop_row = df.copy()
        # fill shop row with shop level data
        shop_row.loc[0, 'shopID'] = shop_data['tsId']
        shop_row.loc[0, 'url'] = shop_data['url']
        # now loop over products and fill df with product level data
        for product in shop_data['products']:
            # skip if the product is an addon (not interesting for the analysis
            # and has different data layout)
            if 'Zugabeartikel' in product['sku']:
                continue
            # set up a df row with shop data, to be filled with products data
            product_row = shop_row.copy()
            product_row.loc[0, 'styleName'] = product['name']
            product_row.loc[0,
                            'overallMark'] = product['qualityIndicators']['reviewIndicator']['overallMark']
            product_row.loc[0, 'overallRating'] = product['qualityIndicators']['reviewIndicator']['overallMarkDescription']

            _, url_parameters = self._parse_url(product['productUrl'])
            size = self._reformat_size(url_parameters['size'])
            product_row.loc[0, 'productSKU'] = product['sku'] + \
                '_' + url_parameters['color'] + '_' + size
            product_row.loc[0, 'productID'] = product['sku']
            product_row.loc[0, 'fabricID'] = product['sku'].split('_')[0]
            product_row.loc[0, 'modelID'] = product['sku'].split('_')[1]
            product_row.loc[0, 'colorID'] = url_parameters['color']
            product_row.loc[0, 'size'] = size

            # now loop over reviews and fill df with review level data
            for review in product['productReviews']:
                # create a copy of the product_row for repeated use (without
                # changing the original)
                review_row = product_row.copy()
                review_row.loc[0, 'reviewID'] = review['uuid']
                review_row.loc[0, 'reviewDate'] = review['creationDate']
                order_date = review['order'].get('orderDate')
                review_row.loc[0,
                               'orderDate'] = order_date if order_date else '2000-01-01'
                review_row.loc[0, 'reviewMark'] = review['mark']
                review_row.loc[0, 'reviewComment'] = review['comment']
                # customer profiles may not always exist or be filled
                # completely
                if review.get('reviewer') and review.get(
                        'reviewer').get('profile'):
                    customerProfile = review['reviewer']['profile']
                    if 'firstname' in customerProfile.keys() and 'lastname' in customerProfile.keys():
                        review_row.loc[0, 'reviewer'] = customerProfile['firstname'] + \
                            '_' + customerProfile['lastname']
                    else:
                        review_row.loc[0, 'reviewer'] = 'unknown'
                    if 'city' in customerProfile.keys():
                        review_row.loc[0,
                                       'reviewerLocation'] = customerProfile['city']
                    else:
                        review_row.loc[0, 'reviewerLocation'] = 'unknown'
                else:
                    review_row.loc[0, 'reviewer'] = 'unknown'
                    review_row.loc[0, 'reviewerLocation'] = 'unknown'
                # add review row to dataframe
                df = pd.concat([df, review_row])

        return df

    def extract(self) -> Generator:
        """Extrahiert Daten aus Trusted-Shops API.

        Yields:
            Generator: Alle Shop-Daten als DataPackage. Name ist die Shop-ID.
        """
        response = httpx.get(self.url(), params=self.parameters)
        try:
            data = response.json().get('response').get('data').get('shop')
            df = self._process_shop_data(data)
            dp = DataPackage(
                name=self.urlvars.get('shop'),
                df=df
            )
            yield dp
        except BaseException:
            log.exception(
                'Trusted Shops Response konnte nicht verarbeitet werden.')
