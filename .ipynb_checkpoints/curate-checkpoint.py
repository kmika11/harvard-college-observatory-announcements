"""
Harvard Library Historical Datasets Curation Functions Module

Functions supporting the processing of files associated with the Trade Statistics volume,
dataset creation, and upload of datafiles. 

Intended to demonstrate pilot of Historic Datasets curation strategy that creates one dataset 
per table series.
"""
import json
import numpy as np
import pandas as pd
import rich
from pyDataverse.models import Dataset
import requests
import dvuploader as dv

def create_dataset_metadata(author, affiliation, contact, email, series_name, series_inventory):
    """
    Create a dictionary of dataset metadata

    Parameters
    ----------
    author : str
        Dataset author name
    affiliation : str
        Dataset athor affiliation
    contact : str
        Dataset contact name (may be same as author)
    email : str
        Dataset contact email address
    series_name : str
        Name of series (e.g., Tonnage: 1)
    series_inventory : DataFrame
        DataFrame containing file metadata

    Return
    ------
    dict
    """
    # Validate parameters
    if ((not author) or
        (not affiliation) or
        (not contact) or
        (not email) or
        (not series_name) or 
        (series_inventory.empty)):
        print('Error: One or more invalid parameter values')
        return {}
    
    # Check the inventory for required fields
    required_fields = ['series_name', 'volume_title', 'contributor', 'subjects', 'card_date_year', 'url']
    for field in required_fields:
        if field not in series_inventory.columns:
            print(f'Error: Missing required field {field} in inventory')
            return {}

    # Get the index of the first file in the inventory
    index = series_inventory.index.values[0]
    
    # Collect metadata variables
    dataset_title = series_inventory.at[index, 'series_name']
    volume_title = series_inventory.at[index, 'volume_title']
    card_number = series_inventory.at[index, 'card_number']
    contributor = series_inventory.at[index, 'contributor']
    all_observations = series_inventory.at[index, 'all_observations']
    collection_date_year = series_inventory.at[index, 'card_date_year']
    collection_date_month = series_inventory.at[index, 'card_date_month']
    collection_date_day = series_inventory.at[index, 'card_date_day']
    collection_date = f'{collection_date_year}-{collection_date_month}-{collection_date_day}'
    
    # Process keywords
    keyword_str = series_inventory.at[index, 'subjects']
    keywords = keyword_str.split(';') if isinstance(keyword_str, str) else []
    kws = [{'keywordValue': kw, 'keywordVocabulary': 'LCSH', 'keywordVocabularyURI': 'https://www.loc.gov/aba/cataloging/subject/'} for kw in keywords]

    # Process topic classes
    topic_class_str = series_inventory.at[index, 'topic_class']
    topics = topic_class_str.split(';') if isinstance(topic_class_str, str) else []
    tps = [{'topicClassValue': top} for top in topics]

    # Process objects
    objects_str = series_inventory.at[index, 'all_observations']
    objects = objects_str.split(';') if isinstance(objects_str, str) else []
    obs = [obj for obj in objects]
    
    # Build the description
    description = f'{dataset_title} is a series of tables and text files associated with HCO Announcement Card number: {card_number}. Compiled by: {contributor}. Objects observed include {all_observations}.'
    
    # Define other metadata
    subjects = 'Astronomy and Astrophysics'
    data_source = [series_inventory.at[index, 'url']]
    astro_facility = ['Harvard Bureau of Astronomical Telegrams']
    astro_type = 'Observation'

    # Build the dataset metadata dictionary
    dataset_metadata = {
        'title': dataset_title,
        'author': [{'authorName': author, 'authorAffiliation': affiliation}],
        'description': [{'dsDescriptionValue': description}],
        'contact': [{'datasetContactName': contact, 'datasetContactAffiliation': affiliation, 'datasetContactEmail': email}],
        'subject': [subjects],
        'license': 'CC0 1.0',
        'keywords': kws,
        'topic_classification': tps,
        'data_source': data_source,
        'creation_date': collection_date,
        'astroObject': obs,
        'astroFacility': astro_facility,
        'astroType': astro_type
    }

    return dataset_metadata
  
    
def create_dataset(ds, dataset_metadata):
    """
    Create a dataverse dataset using easyDataverse.
    Note that metadata fields are hardcoded to reflect dataset's requirements. 

    Parameters
    ----------
    ds : initialized easyDataverse Dataset
    dataset_metadata : dict
        Dictionary of dataset metadata values

    Return
    ------
    dict: 
        {status: bool, dataset_id: int, dataset_pid: str}

    """
    # validate parameters
    if ((not ds) or
        (not dataset_metadata)):
        return {
            'status':False, 
            'dataset_id':-1, 
            'dataset_pid':''
        }

    # populate the dataset model with metadata values
    ds.citation.title = dataset_metadata.get('title')

    for authors in dataset_metadata.get('author'):
        ds.citation.add_author(name = authors['authorName'],
                              affiliation = authors['authorAffiliation'])

    for desc in dataset_metadata.get('description'):
        ds.citation.add_ds_description(value=desc['dsDescriptionValue'])
    
    for contact in dataset_metadata.get('contact'):
        ds.citation.add_dataset_contact(name = contact['datasetContactName'],
                                        email = contact['datasetContactEmail'])

    ds.citation.subject = dataset_metadata.get('subject')

    for keyword in dataset_metadata.get('keywords'):
        ds.citation.add_keyword(value = keyword['keywordValue'],
                                vocabulary = keyword['keywordVocabulary'],
                                vocabulary_uri = keyword['keywordVocabularyURI'])

    for topic in dataset_metadata.get('topic_classification'):
            ds.citation.add_topic_classification(value = topic['topicClassValue'])

    data_sources = dataset_metadata.get('data_source')
    if pd.notna(data_sources):
        ds.citation.data_sources = data_sources
        
    ds.citation.distribution_date = dataset_metadata.get('creation_date')
    ds.astrophysics.astro_object = dataset_metadata.get('astroObject')
    ds.astrophysics.astro_facility = dataset_metadata.get('astroFacility')


    #dict = rich.print(ds.dataverse_dict())
    return ds
    
def pydataverse_create_dataset(api, dataverse_url, dataset_metadata):
    """
    Create a dataverse dataset

    Parameters
    ----------
    api : pyDataverse API
    dataverse : str
        Name of dataverse collection url (e.g., https://demo.dataverse.org/dataverse/histd)
    dataset_metadata : dict
        Dictionary of dataset metadata values

    Return
    ------
    dict: 
        {status: bool, dataset_id: int, dataset_pid: str}

    """
    # validate parameters
    if ((not api) or
        (not dataset_metadata)):
        return {
            'status':False, 
            'dataset_id':-1, 
            'dataset_pid':''
        }

    # create the pyDataverse dataset model
    ds = Dataset()
    # populate the dataset model with metadata values
    ds.title = dataset_metadata.get('title')
    ds.author = dataset_metadata.get('author')
    ds.dsDescription =  dataset_metadata.get('description')
    ds.datasetContact = dataset_metadata.get('contact')
    ds.subject = dataset_metadata.get('subject')
    ds.license = dataset_metadata.get('license')
    ds.keyword = dataset_metadata.get('keywords')
    ds.topicClassification = dataset_metadata.get('topic_classification')
    ds.dataSources = dataset_metadata.get('data_source')
    ds.distributionDate = dataset_metadata.get('creation_date')
    ds.astroObject = dataset_metadata.get('astroObject')
    ds.astroFacility = dataset_metadata.get('astroFacility')
    ds.universe = dataset_metadata.get('universe') 
    
    # use pyDataverse to ensure that the metadata is valid
    if (ds.validate_json() == False):
        return {
            'status':False, 
            'dataset_id':-1, 
            'dataset_pid':''
        }

    # 
    # create the dataset via the dataverse api
    #

    # get the base url
    base_url = api.base_url
    # get the api token
    api_token = api.api_token
    # dataverse collection url 
    dataverse_url = dataverse_url
    # create the headers
    headers = {'X-Dataverse-key': api_token, 'Content-Type' : 'application/json'}
    # create the request url
    request_url = '{}/api/dataverses/{}/datasets'.format(base_url, dataverse_url)

    # call the requests library using the request url
    response = requests.post(request_url, headers=headers, data=ds.json())
    # get the status and message from the response
    status = int(response.status_code)

    # handle http responses
    if (not ((status >= 200) and
        (status < 300))):
        print('Error: {} - failed to create dataset {}'.format(status, dataset_metadata.get('title')))
        return {
            'status':False, 
            'dataset_id':-1, 
            'dataset_pid':''
        }
    # if success
    return {
        'status':True, 
        'dataset_id':response.json().get('data').get('id'),
        'dataset_pid':response.json().get('data').get('persistentId')     
    }

def create_datafile_metadata(inventory_df, template_csv, template_txt, template_xml):
    """
    Create metadata for open metadata project datafiles based upon a template

    Parameters
    ----------
    inventory_df : DataFrame
        DataFrame containing list of datafiles to upload

    template_csv : str
        String used to generate metadata to be applied to each csv file in the inventory
        
    template_txt : str
        String used to generate metadata to be applied to each txt file in the inventory

    Return
    -------
    DataFrame
    """

    # validate parameters
    if ((inventory_df.empty == True) or
        (not template_csv) or 
        (not template_txt)):
        print('Error: One or more invalid parameters')
        return pd.DataFrame()

    # check the dataframe for required fields
    if ((not 'filename' in inventory_df.columns) or
        (not 'file_type' in inventory_df.columns) or  
        (not 'series_name' in inventory_df.columns) or
        (not 'observation' in inventory_df.columns)):
        print('Error: One or more missing required fields in inventory')
        return pd.DataFrame()

    #
    # prepare series of values to add to metadata dataframe
    #

    # prepare file name for actual file
    all_filenames = []
    # prepare file types
    all_file_types = []
    # prepare datafile descriptions
    all_descriptions = []
    # prepare datafile tags
    all_tags = []
    # prepare mimetypes
    all_mime_types = []

    # iterate through through the inventory and create datafile metadata
    for index, row in inventory_df.iterrows():
        # get inventory variables
        filename = row['filename']
        all_filenames.append(filename)
        series_name = row['series_name']
        file_type = row['file_type']
        all_file_types.append(file_type)
        file_tags = ['Data'] # tags for this particular file, init with 'Data'

        #get file entities:
        observations = row['observation']
        if pd.notna(observations):
            entities = str(observations).split(';')
            file_tags.extend(entities)

        # set file mimetype
        if (file_type == 'jpg'):
            all_mime_types.append('image/jpeg')
        elif (file_type == 'xml'):
            all_mime_types.append('application/xml')
        elif (file_type == 'txt'):
            all_mime_types.append('text/plain')        
        elif (file_type == 'csv'):
            all_mime_types.append('text/csv')
        else:
            all_mime_types.append('UNKNOWN')             

        # handle csv files
        if (file_type == 'csv'):
            # table title for csv files included in descriptions
            desc = template_xml + ' ' + series_name
        if (file_type == 'xml'):
            desc = template_xml + ' ' + series_name
        else:
            # set description
            desc = template_txt + ' ' + series_name
        all_descriptions.append(desc)

        # serialize the file tags
        all_tags.append(file_tags)

    # Build dataframe
    df = pd.DataFrame({
        'filename': all_filenames,
        'file_type': all_file_types,
        'description': all_descriptions,
        'mimetype': all_mime_types,
        'tags': all_tags
    })

    return df

def python_dvuploader(api, dataverse_url, dataset_pid, data_directory, metadata_df):
    """
    Upload data files to dataverse repository using direct upload method

    Parameters
    ----------
    api : pyDataverse api
    dataverse_url : str
        Dataverse installation url (e.g., https://demo.dataverse.org)
    dataset_pid : str
        Persistent identifier for the dataset (its DOI, takes form: doi:xxxxx)
    data_directory : str
        Directory where data files are kept
    metadata_df : DataFrame
        DataFrame containing metadata about datafiles to upload

    Return
    ------
    dict
        {upload: bool, errors: list, finalize: bool}
    """

    #validate params
    if ((not api) or 
        (not dataverse_url) or
        (not dataset_pid) or
        (not data_directory) or
        (metadata_df.empty==True)):
        return False

    #error msg
    errors = []

    json_data = []
    cats = None

    #add each file in metadata_df to files list for dvuploader
    
    files = []
    
    for row in metadata_df.iterrows():
        file = row[1].get('filename')
        filepath = data_directory + "/" + file
        file_name = row[1].get('filename')
        desc = row[1].get('description')
        mime_type = row[1].get('mimetype')

        #format tags
        tags = row[1].get('tags')
        tags_lst = eval(tags)
        
        files.append(dv.File(filepath = filepath,
                             file_name = file_name,
                             description = desc,
                             mimeType = mime_type,
                             categories = tags_lst
                            )
                    )
        
        #print('Uploading: {}/{} - {} {}'.format(data_directory, filepath, desc, mime_type))

        
    key = api.api_token
    dvuploader = dv.DVUploader(files=files)
        
    dvuploader.upload(
        api_token = key,
        dataverse_url = dataverse_url,
        persistent_id = dataset_pid,
        n_parallel_uploads= 2 #however many your installation can handle
    )

    
def delete_datasets(api, dataverse_url):
    """
    Delete all datasets in the dataverse collection. 
    Use with caution, and only on demo.dataverse.org installation.

    Parameters
    ----------
    api : pyDataverse API
    dataverse_url : str
        Name of the dataverse collection (e.g., histd)

    Return
    ------
        bool
    """
    # get the datasets in the collection
    contents = api.get_dataverse_contents(dataverse_url, auth=True)
    # get the data
    data = contents.json().get('data')
    datasets = []
    for dataset in data:
        url = dataset.get('persistentUrl')
        pid = url.split('https://doi.org/')[1]
        datasets.append('doi:' + pid)
    # destroy the datasets
    for dataset in datasets:
        response = api.destroy_dataset(dataset, is_pid=True, auth=True)
        status = response.json()
        print('api.destroy_dataset: {}'.format(status))
    return True

def publish_datasets(api, dataverse_collection, version='major'):
    """
    Publish each dataset in a list. Logs result to log dataframe

    Parameters
    ----------
    api : pyDataverse API
    dataverse_url : str
        Name of the dataverse collection (e.g., histd)
    version: str
        Type of version update to trigger. "major" or "minor"

    Return
    ------
    dict
        {'status':bool,'message':str}
    """
    # validate parameters
    if ((not api) or
        (not dataverse_collection)):
        return {'status':False,'message':'Invalid parameter'}
    
    # get the datasets in the collection
    contents = api.get_dataverse_contents(dataverse_collection, auth=True)
    # get the data
    data = contents.json().get('data')
    datasets = []
    for dataset in data:
        url = dataset.get('persistentUrl')
        pid = url.split('https://doi.org/')[1]
        datasets.append('doi:' + pid)

    # store errors to return, keyed on pid
    errors = {}

    import requests
    # get the base url
    base_url = api.base_url
    # get the api token
    api_token = api.api_token
    # create the headers
    headers = {'X-Dataverse-key': api_token, 'Content-Type' : 'application/json'}

    # publish the datasets
    for dataset in datasets:
        # create the request url
        request_url = '{}/api/datasets/:persistentId/actions/:publish?persistentId={}&type={}'.format(base_url, dataset, version) 
        # call the requests library using the request url
        response = requests.post(request_url, headers=headers)
        
        # handle responses
        status = response.status_code
        if (not (status >= 200 and status < 300)):
            msg = 'publish_dataset::Error - failed to publish dataset: {}:{}'.format(status,dataset)
            errors[dataset] = {'status':False,'message':msg}
        else:
            msg = 'publish_dataset::Success - published dataset: {}:{}'.format(status,dataset)
            errors[dataset] = {'status':True,'message':msg}
            
    return errors

def unlock_datasets(api, dataverse_collection):
    """
    Unlock datasets that failed to publish. Usually this is due to some kind of Dataverse indexing error, not user error)

    Parameter
    ---------
    api : pyDataverse api
    dataverse_collection: str
        ID of the dataverse collection (e.g., 1924_trade_returns)

    Return
    ------
    dict
        {'status':bool,'message':str}
    """
    
    # validate parameters
    if ((not api) or
        (not dataverse_collection)):
        return {'status':False,'message':'Invalid parameter'}
    
    # get the datasets in the collection
    contents = api.get_dataverse_contents(dataverse_collection, auth=True)
    # get the data
    data = contents.json().get('data')
    datasets = []
    for dataset in data:
        url = dataset.get('persistentUrl')
        pid = url.split('https://doi.org/')[1]
        datasets.append('doi:' + pid)

    import requests
    # get the base url
    base_url = api.base_url
    # get the api token
    api_token = api.api_token
    # create the headers
    headers = {'X-Dataverse-key': api_token, 'Content-Type' : 'application/json'}
    
    #create list of locked datasets
    locked_pids = []
    
    #check dataset to see if locked & if so add to list of datasets to unlock
    for dataset in datasets: 
        # create the request url
        request_url = '{}/api/datasets/:persistentId/locks?persistentId={}'.format(base_url, dataset) 
        # call the requests library using the request url
        response = requests.get(request_url, headers=headers)
        #check status response to see if there's a lock
        ret = response.json()['data']
        if ret:
            locked_pids.append(ret[0]['dataset'])
        else:
            continue

    #store errors to return, keyed on pid
    errors = {}    
    
    #unlock list of datasets
    for pid in locked_pids:
        #create new request url
        request_url = '{}/api/datasets/:persistentId/locks?persistentId={}'.format(base_url, pid)
        #call the requests library using the request url
        response = requests.delete(request_url, headers=headers)
        #save status reponse for error return
        status = response.status_code
        #add messages depending on status response
        if (not (status >= 200 and status < 300)):
            msg = 'publish_dataset::Error - failed to unlock dataset: {}:{}'.format(status,pid)
            errors[pid] = {'status':False,'message':msg}
        else:
            msg = 'publish_dataset::Success - unlocked dataset: {}:{}'.format(status,pid)
            errors[pid] = {'status':True,'message':msg}
            
    return errors

# end document