# -*- coding: utf-8 -*-
"""
Created on Sunday, 19th December 2021 7:54:09 pm
===============================================================================
@filename:  wrapper.py
@author:    Manuel Martinez (manmartgarc@gmail.com)
@project:   qualpy
@purpose:   module that contains the main wrapper class for the Qualtrics API
===============================================================================
"""
import io
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import pandas as pd
import requests


@dataclass
class QualtricsAPI:
    base_url: str
    library_id: str
    api_token: str = field(repr=False, default=os.environ['QUAL_APIKEY'])

    def __post_init__(self) -> None:
        self._headers = {
            'content-type': 'application/json',
            'x-api-token': self.api_token
        }

        # cache mailing lists
        self.mls = self._list_mailing_lists()

    def create_mailing_list(self,
                            name: str,
                            category: Optional[str] = None) -> str:
        """
        creates a mailing list under the libraryId passed at class
        instantiation

        Args:
            name (str): mailing list name
            category (Optional[str], optional): folder to place the mailing
                list. Defaults to None.

        Returns:
            str: returns the created mailing list's id
        """
        # check if mailing list exists
        try:
            result = self.get_mailing_list_id(mlname=name)
        except KeyError:
            requests.post(
                url=self._build_url('mailinglists'),
                headers=self._headers,
                json=dict(name=name,
                          libraryId=self.library_id,
                          category=category))

            # update cache
            self.mls = self._list_mailing_lists()
            result = self.get_mailing_list_id(name)

        return result

    def import_contacts(self, mlname: str, contacts: pd.DataFrame, **kwargs
                        ) -> requests.Response:
        """
        imports contacts to a specific mailing list

        Args:
            mailing_list_name (str): name of mailing list
            contacts (pd.DataFrame): contact information

        Returns:
            requests.Response: upload response object
        """
        mlid = self.get_mailing_list_id(mlname)
        contacts = self._convert_df_to_contacts(df=contacts, **kwargs)

        url = self._build_url('mailinglists', mlid, 'contactimports')
        response = requests.post(url=url,
                                 headers=self._headers,
                                 json={'contacts': contacts})

        return response

    def get_all_surv_data(self, **kwargs) -> pd.DataFrame:
        """
        gets both finished and in progress responses for a single survey

        Returns:
            pd.DataFrame: response data
        """
        auxs: list = []
        for state in [False, True]:
            aux = self._get_surv_data(finished_only=state, **kwargs)
            aux['finished'] = state
            auxs.append(aux)

        return pd.concat(auxs)

    def get_all_distribution_data(self,
                                  surv_name: str,
                                  verbose: bool = False) -> pd.DataFrame:
        """
        gets all email distribution data for a specific survey

        Args:
            surv_name (str): survey name
            verbose (bool): verbose pagination report

        Returns:
            pd.DataFrame: email distribution detail
        """
        mailing_lists = self._get_distribution_ids(surv_name=surv_name)

        auxs = []
        for ml_id in mailing_lists.keys():
            aux = self._get_distribution(ml_id=ml_id, verbose=verbose)
            auxs.append(aux)

        df = pd.concat(auxs, ignore_index=True)
        df.columns = df.columns.str.lower()

        return df

    def get_mailing_list_id(self, mlname: str) -> str:
        """
        fetches the mailing list id of a mailing list name

        Args:
            mlname (str): mailing list name

        Raises:
            ValueError: if the mailing list name is not found in the mailing
                lists that are in qualtrics.

        Returns:
            str: mailing list id
        """
        try:
            mls = self._get_mailing_list_attr(mlname, 'id')
        except (ValueError, KeyError) as e:
            raise e

        return mls

    def _build_url(self, *args) -> str:
        """
        joins the arguments pased with `/` into the base url

        Returns:
            str: built url
        """
        return '/'.join([self.base_url, *args])

    def _get_surv_data(self,
                       surv_name: str,
                       verbose: bool = True,
                       finished_only: bool = True,
                       embedded_fields: list[str] = []
                       ) -> pd.DataFrame:
        """
        export current data for a specific survey id into a pandas dataframe

        Args:
            surv_name (str): survey name
            verbose (bool, optional): whether progress is printed
            complete (bool, optional): only export completed responses

        Raises:
            Exception: download failed for some reason

        Returns:
            pd.DataFrame: survey responses
        """
        # set up values
        prog_status = 'inProgress'
        url = self._build_url('surveys',
                              self._get_survey_id(surv_name),
                              'export-responses')

        data_kwds = {'format': 'csv',
                     'useLabels': True,
                     'exportResponsesInProgress': not finished_only,
                     'compress': True,
                     'embeddedDataIds': embedded_fields}
        ready = None

        # start up download from server
        dl_req = requests.post(url=url,
                               headers=self._headers,
                               json=data_kwds)

        prog_id = dl_req.json()['result']['progressId']
        prog_status = dl_req.json()['result']['status']

        # check on progress
        while prog_status != 'complete' and \
                prog_status != 'failed' and \
                ready is None:
            req_check_url = self._build_url('surveys',
                                            self._get_survey_id(surv_name),
                                            'export-responses',
                                            prog_id)
            req_check_resp = requests.get(url=req_check_url,
                                          headers=self._headers)
            req_check_prog = req_check_resp.json()['result']['percentComplete']

            if ready is None and verbose:
                print(f'Progress = {req_check_prog:.2f}')

            try:
                ready = req_check_resp.json()['result']['fileId']
            except KeyError:
                pass

            prog_status = req_check_resp.json()['result']['status']

        if prog_status == 'complete' and verbose:
            print('Progress = Done')

        # check for errors
        if prog_status == 'failed':
            raise Exception('export failed')

        file_id = req_check_resp.json()['result']['fileId']

        # download file
        req_dl_url = self._build_url('surveys',
                                     self._get_survey_id(surv_name),
                                     'export-responses',
                                     file_id,
                                     'file')
        req_dl = requests.get(url=req_dl_url,
                              headers=self._headers,
                              stream=True)

        # extract from zip into pandas dataframe
        df = pd.read_csv(io.BytesIO(req_dl.content),
                         compression='zip',
                         low_memory=False)

        # drop qualtric headers
        df = df.iloc[2:, :].copy()
        df.reset_index(inplace=True, drop=True)

        # lowercase cols
        df.columns = df.columns.str.lower()

        return df

    def _get_distribution_ids(self, surv_name: str) -> dict:
        """
        gets distribution ids for an associated survey. this only gets
        invites, leaving OUT reminders and tests.

        Args:
            surv_name (str): survey name to pull the ids from

        Returns:
            dict: dictionary with distribution ids and date sents (ISO)
        """
        r = requests.get(url=self._build_url('distributions'),
                         headers=self._headers,
                         params={'surveyId': self._get_survey_id(surv_name)})
        results = r.json()['result']['elements']

        dists: dict[str, dict] = {}
        for result in results:
            resultType = result['requestType']
            sampleId = result['recipients']['sampleId']
            dist_id = result['id']
            ml_id = result['recipients']['mailingListId']
            send_date = result['sendDate']

            if (resultType == 'Invite') & (sampleId is None):
                dists[ml_id] = {'dist_id': dist_id}
                dists[ml_id]['send_date'] = send_date

        # return dists
        return dists

    def _get_distribution(self,
                          ml_id: str,
                          verbose: bool = False) -> pd.DataFrame:
        """
        gets distribution details

        Args:
            surv_name (str): survey name
            ml_id (str): mailing list id

        Returns:
            pd.DataFrame: distribution history
        """
        url = self._build_url('mailinglists', ml_id, 'contacts')
        r = requests.get(url, headers=self._headers)
        nextPage = r.json()['result']['nextPage']

        # parse request
        dfs = []
        dfs.extend(self._parse_distribution_page(r))

        count: int = 0
        while nextPage is not None:
            r = requests.get(nextPage, headers=self._headers)
            data = r.json()

            count += 1
            nrecords = len(data['result']['elements'])

            if verbose:
                print(f'Working on page {count} with {nrecords} records.')

            dfs.extend(self._parse_distribution_page(r))
            nextPage = data['result']['nextPage']

        # # cast into a df
        df = pd.DataFrame(dfs)

        return df

    def _get_dist_links(self,
                        dist_id: str,
                        surv_name: str,
                        verbose: bool = False) -> pd.DataFrame:
        """
        get distribution links

        Args:
            surv_name (str): survey name
            dist_id (str): distribution id
            verbose (bool): print pagination step. Defaults to False

        Returns:
            pd.DataFrame: distribution links with contact ids
        """
        url = self._build_url('distributions', dist_id, 'links')
        r = requests.get(url,
                         headers=self._headers,
                         params={'surveyId': self._get_survey_id(surv_name)})
        nextPage = r.json()['result']['nextPage']
        count = 1

        # parse request
        dfs = []
        dfs.append(pd.DataFrame(r.json()['result']['elements']))
        while nextPage is not None:
            for attempt in range(10):
                try:
                    count += 1
                    r = requests.get(nextPage, headers=self._headers)
                    df = pd.DataFrame(r.json()['result']['elements'])
                    dfs.append(df)
                    nextPage = r.json()['result']['nextPage']
                    if verbose:
                        print(
                            f'Working on page {count} with {len(df)} records.')
                except KeyError:
                    count += 1
                    r = requests.get(nextPage, headers=self._headers)
                    df = pd.DataFrame(r.json()['result']['elements'])
                    dfs.append(df)
                    nextPage = r.json()['result']['nextPage']
                    if verbose:
                        print(
                            f'Working on page {count} with {len(df)} records.')
                else:
                    break
            else:
                raise KeyError(
                    'Tried 10 attempts with a failure.')

        # # cast into a df
        df = pd.concat(dfs, ignore_index=True)
        df.columns = df.columns.str.lower()

        return df

    def _list_mailing_lists(self) -> dict[str, dict]:
        """
        Get all mailing lists objects in Qualtrics

        Returns:
            dict[str, Dict]: A dictionary where keys are the names of mailing
                lists and values are dictionaries that hold the information
                associated to each specific mailing list.

        Raises:
            ValueError: this is raised when duplicated mailing list names are
                detected on the Qualtrics account used via API key.
        """
        response = requests.get(
            url=self._build_url('mailinglists'),
            headers=self._headers)

        mailing_lists = response.json()['result']['elements']

        # check for duplicates
        names = set(ml['name'] for ml in mailing_lists)

        if len(names) != len(mailing_lists):
            raise ValueError(
                'There are duplicate names for mailing lists. This program '
                'assumes that the names are unique and casts these into a set '
                'therefore some information might be lost. Please make sure '
                'that there are no duplicate names for mailing lists.'
            )

        results = {ml.pop('name'): ml for ml in mailing_lists}

        return results

    def _list_surveys(self) -> requests.Response:
        """
        Returns meta data for all surveys available to the user.

        Returns:
            requests.Response: response object
        """
        r = requests.get(url=self._build_url('surveys'), headers=self._headers)

        return r

    def _get_survey_id(self, suname: str) -> str:
        """
        fetches the survey id corresponding to a survey name

        Args:
            suname (str): survey name

        Raises:
            ValueError: if the survey name is not found in the surveys
                associated with this particular qualtrics account

        Returns:
            str: survey id
        """
        surveys = self._list_surveys().json()['result']['elements']
        suid = None
        for survey in surveys:
            if survey['name'] == suname:
                suid = survey['id']
                break
        if suid is None:
            raise ValueError('survey does not exist')

        return suid

    def _get_mailing_list_attr(self, mlname: str, key: str) -> str:
        """
        gets a mailing list attribute from the cache at the instance level.

        Args:
            mlname (str): mailing list name to search
            key (str): key to get {`libraryId`, `id`, `category`, `folder`}

        Raises:
            e: whether mailing list name cannot be found or if they cannot
                be found

        Returns:
            str: attribute value
        """
        try:
            value = self.mls[mlname][key]

            return value
        except (ValueError, KeyError) as e:
            raise e

    def _convert_df_to_contacts(self,
                                df: pd.DataFrame,
                                emailcol: str,
                                langcol: Optional[str] = None,
                                ecols: Optional[list[str]] = None,
                                ) -> list[dict]:
        """
        converts a pandas dataframe into a list of dictionaries containing
        each of the contact's parameters

        Args:
            df (pd.DataFrame): contact info
            emailcol (str): column containing emails
            langcol (str): column containing language. Defaults to None.
            ecols (Optional[list[str]], optional): set of columns that will
                be uploaded as embedded data. Defaults to None.

        Returns:
            list[dict]: contacts object
        """
        assert len(df) <= 10000, 'maximum contacts to import at once is 10000'

        if isinstance(ecols, str):
            ecols = [ecols]

        contacts = []
        for row in df.itertuples():
            contact_data = {}
            contact_data['email'] = getattr(row, emailcol)
            if langcol is not None:
                contact_data['language'] = getattr(row, langcol)
            if ecols is not None:
                edata = dict(zip(ecols, [getattr(row, col) for col in ecols]))
                contact_data['embeddedData'] = edata
            contacts.append(contact_data)

        # check that emails are unique
        assert len(set(contact['email'] for contact in contacts)) == len(df)

        return contacts

    def _create_distribution_links(self,
                                   suname: str,
                                   mlname: str
                                   ) -> requests.Response:
        """
        generates distribution links without sending them

        Args:
            suname (str): survey name
            mlname (str): mailing list name

        Returns:
            requests.Response: response object
        """
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        mlid = self.get_mailing_list_id(mlname)
        r = requests.post(url=self._build_url('distributions'),
                          headers=self._headers,
                          json=dict(surveyId=self._get_survey_id(suname),
                                    linkType='Individual',
                                    action='CreateDistribution',
                                    description=f'dist {ts}',
                                    mailingListId=mlid))

        return r

    def _parse_distribution_page(self, r: requests.Response) -> list:
        p_df = []

        for person in r.json()['result']['elements']:
            p_dict = {
                'contactid': person['id'],
                'email': person['email'],
                'language': person['language'],
                'unsubscribed': person['unsubscribed'],
            }

            for email in person['emailHistory']:
                p_df.append(p_dict | email)

        return p_df
