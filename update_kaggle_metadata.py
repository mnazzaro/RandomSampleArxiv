'''
File: update_kaggle_metadata.py
  Read recent oai updates from arXiv's oai api.
  Sort by paper_id.
  Merge with an existing file of previous paper metadata.
Loads only new data from the past week.
Kaggle users have mentioned an interest in getting updates.
A single file like this may keep things simple.
The oai api is scheduled for daily maintenance between 0:00 and 1:00 UTC
Adding created and updated to Kaggle metadata dataset.
In the oai xml:
  - each version has a date field, which matches the created column.
  - the header/datestamp seems to match the max(updated) column
  https://export.arxiv.org/oai2?verb=GetRecord&identifier=oai:arXiv.org:2005.13766&metadataPrefix=arXivRaw
  select paper_id, created, updated, modtime 
    from arXiv_metadata 
   where paper_id = '2005.13766';
  +------------+---------------------+---------------------+------------+
  | paper_id   | created             | updated             | modtime    |
  +------------+---------------------+---------------------+------------+
  | 2005.13766 | 2020-05-28 03:43:31 | 2020-05-29 00:07:30 | 1590710850 |
  | 2005.13766 | 2020-05-30 23:12:48 | 2020-06-02 00:15:35 | 1591056935 |
  | 2005.13766 | 2020-08-01 23:02:52 | 2020-08-04 00:16:58 | 1596500217 |
  +------------+---------------------+---------------------+------------+
  The modtime epoch 1596500217 == GMT: Tuesday, August 4, 2020 12:16:57 AM
  Adding the version create date.
  Adding the latest paper update date.
  Adding license. Sometimes it's empty.
  Replace the versions string, with an array of hashes.
References:
  https://arxiv.org/help/bulk_data
  https://arxiv.org/help/oa/index
  https://export.arxiv.org/oai2?verb=GetRecord&identifier=oai:arXiv.org:2005.13766&metadataPrefix=arXivRaw
  https://export.arxiv.org/oai2?verb=Identify
    - <deletedRecord>persistent</deletedRecord>
  https://github.com/arXiv/arxiv-base/blob/develop/arxiv/util/authors.py
  https://github.com/arXiv/arxiv-base/blob/develop/arxiv/util/tex2utf.py
  https://github.com/arXiv/arxiv-browse/blob/develop/browse/services/util/formats.py#L45
  https://github.com/mattbierbaum/arxiv-public-datasets/blob/master/arxiv_public_data/oai_metadata.py#L179
  https://www.openarchives.org/OAI/openarchivesprotocol.html#SelectiveHarvestingandDatestamps
  https://www.openarchives.org/OAI/openarchivesprotocol.html#Dates
  https://www.openarchives.org/OAI/openarchivesprotocol.html#DeletedRecords
    - the repo maintains deletions with no time limit, the full history.
TODO: check what happens with withdrawn/deleted 
TODO: Is category ordered?
TODO: Ok to share script?
'''

import argparse
import json
import os
import requests
import sys
import time
import unittest
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from shutil   import move 


DATA_DIR       = '/home/markn/Documents/arxiv/TexToHtml/DataDir'
F_JSON         = f'{ DATA_DIR }/arxiv-metadata-oai-snapshot.json'
F_TMP_UPDATES  = f'{ DATA_DIR }/tmp_updates.json'
F_TMP_SORTED   = f'{ DATA_DIR }/tmp_updates_sorted.json'
F_TMP_MERGED   = f'{ DATA_DIR }/tmp_merged.json'
FROM_DAYS_AGO  = 7
OAI_XML_NAMESPACES = {
  'OAI'  : 'http://www.openarchives.org/OAI/2.0/',
  'arXiv': 'http://arxiv.org/OAI/arXivRaw/'
}
URL_ARXIV_OAI = 'https://export.arxiv.org/oai2'

args = None

def main():
  parse_args()
  info(f'Starting { sys.argv[0] } | { args.resumption_token }')
  debug(f'Starting { sys.argv } | { sys.argv }')
  debug(f'Arg | args.json_file        : { args.json_file }')
  debug(f'Arg | args.tmp_updates_file : { args.tmp_updates_file }')
  debug(f'Arg | args.tmp_sorted_file  : { args.tmp_sorted_file }')
  debug(f'Arg | args.tmp_merged_file  : { args.tmp_merged_file }')
  debug(f'Arg | args.verbose          : { args.verbose }')

  download_updates()
  sort_the_updates_file()
  merge_files()
  check_merged_then_overwrite_original()


def check_merged_then_overwrite_original():
  json_lines       = sum(1 for line in open(args.json_file))
  tmp_merged_lines = sum(1 for line in open(args.tmp_merged_file))
  info(f'Comparing file lines:   json:{ json_lines:9}')
  info(f'Comparing file lines: merged:{ tmp_merged_lines:9}')

  json_file_size       = os.stat(args.json_file).st_size
  tmp_merged_file_size = os.stat(args.tmp_merged_file).st_size
  info(f'Comparing file size:   json: { json_file_size }')
  info(f'Comparing file size: merged: { tmp_merged_file_size }')

  if json_lines < tmp_merged_lines:
    move(args.tmp_merged_file, args.json_file)
    info(f'Moved: { args.tmp_merged_file } to { args.json_file }')
  pass


def debug(s, min_verbose=1):
  if args.verbose >= min_verbose:
    print( f'{ s }' )


def download_updates():
  rt = args.resumption_token if args.resumption_token else None

  chunk_index   = 0
  total_records = 0
  more_oai_updates = True
  while more_oai_updates:
    xml = get_listrecord_chunk(rt)

    rt, c = parse_and_save_listrecord_as_json(xml, rt)
    chunk_index   = chunk_index + 1
    total_records = total_records + c
    info(f'{ chunk_index:4} | Records { total_records:7} | { rt }')

    if not rt:
      more_oai_updates = False


def get_listrecord_chunk(
    resumption_token = None,
    harvest_url      = URL_ARXIV_OAI,
    metadata_prefix  = 'arXivRaw',
  ):
  ''' Query arXiv's OAI API for first 1000 papers.
        If resumption_token included, get the next batch of papers. '''

  parameters = {'verb': 'ListRecords'}
  if resumption_token:
    parameters['resumptionToken'] = resumption_token
  else:
    parameters['metadataPrefix'] = metadata_prefix
    if not args.reload_all:
      parameters['from'] = f'{ date.today() - timedelta(days=args.from_days_ago) }'
      info(f'Loading metadata starting { args.from_days_ago } days ago (from { parameters["from"] })')

  response = requests.get(harvest_url, params=parameters)

  if response.status_code == 200:
    time.sleep(12)                     # OAI server usually requires a 10s wait
    return response.text

  elif response.status_code == 503:
    secs = int(response.headers.get('Retry-After', 20)) * 1.5
    info(f'Requested to wait, waiting { secs } seconds until retry...')
    time.sleep(secs)
    return get_list_record_chunk(resumption_token=resumption_token)

  else:
    raise Exception(
      f'''Unknown error in HTTP request { response.url }, 
      status code: { response.status_code }
      '''
    )

                                       # Extract paper_id from json
                                       # e.g.:  {"id":"1408.5307","sub...
def get_json_paper_id(s):
  return s[7 : s.find('"',7)]


def info(s):
  print( f'{ datetime.today() } { s }' )


def merge_files():
  ''' Useful for merging weekly updates into a main arXiv dataset file.
      Expects:
        - files to be sorted as the unix sort utility would order them.
        - to sort on the arXiv paperId/"id".
        - files to contain json, begin with key "id", and have no whitespace.
  '''
  json_file       = open(args.json_file)
  tmp_sorted_file = open(args.tmp_sorted_file)
  tmp_merged_file = open(args.tmp_merged_file, 'w')

  count_json_papers     = 0
  count_merged_papers   = 0
  count_new_papers      = 0
  count_replaced_papers = 0
  count_sorted_papers   = 0
  count_identical_lines = 0

                                       # todo: fix for first run on json
  json_string         = json_file.readline()
  json_paper_id       = get_json_paper_id(json_string)
  tmp_sorted_string   = tmp_sorted_file.readline()
  tmp_sorted_paper_id = get_json_paper_id(tmp_sorted_string)

                                       # read to the end of each file
  while json_paper_id or tmp_sorted_paper_id:
    advance_json_file   = False
    advance_sorted_file = False
    write_json_string   = False
    write_sorted_string = False

                                       # when only one file has remaining papers
    if not json_paper_id:
      advance_sorted_file = True
      write_sorted_string = True
      count_sorted_papers = count_sorted_papers + 1
    elif not tmp_sorted_paper_id:
      advance_json_file   = True
      write_json_string   = True
      count_json_papers   = count_json_papers + 1
    else:
                                       # mostly, existing records are copied over:
      if json_paper_id < tmp_sorted_paper_id:
        write_json_string   = True
        advance_json_file   = True
        count_json_papers   = count_json_papers + 1

                                       # replace metadata from the sorted updates:
      elif json_paper_id == tmp_sorted_paper_id:
        write_sorted_string   = True
        advance_sorted_file   = True
        advance_json_file     = True
        count_json_papers     = count_json_papers     + 1
        count_sorted_papers   = count_sorted_papers   + 1
        count_replaced_papers = count_replaced_papers + 1

        if json_string == tmp_sorted_string:
          count_identical_lines = count_identical_lines + 1

                                       # The sorted file has a new paper:
      elif tmp_sorted_paper_id < json_paper_id:
        write_sorted_string = True
        advance_sorted_file = True
        count_sorted_papers = count_sorted_papers + 1
        count_new_papers    = count_new_papers    + 1

                                       # write lines to new merged file:
    if write_json_string:
      print(json_string, file=tmp_merged_file, end='')
      count_merged_papers = count_merged_papers + 1
    if write_sorted_string:
      print(tmp_sorted_string, file=tmp_merged_file, end='')
      count_merged_papers = count_merged_papers + 1

                                       # then advance in files for existing and updated:
    if advance_json_file:
      json_string   = json_file.readline()
      json_paper_id = get_json_paper_id(json_string)
    if advance_sorted_file:
      tmp_sorted_string   = tmp_sorted_file.readline()
      tmp_sorted_paper_id = get_json_paper_id(tmp_sorted_string)
     

  json_file.close()
  tmp_sorted_file.close()
  tmp_merged_file.close()

  info( f'Sorted    lines: { count_sorted_papers:9}')
  info( f'New       lines: { count_new_papers:9}')
  info( f'Edited    lines: { count_replaced_papers:9}')
  info( f'Identical lines: { count_identical_lines:9}')
  info( f'Original  lines: { count_json_papers:9}')
  info( f'Merged    lines: { count_merged_papers:9}')


def parse_and_save_listrecord_as_json(xml, resumption_token):
  ''' Convert oai xml, and look for next token '''

  count_records = 0
  mode = 'a' if resumption_token else 'w'
  with open(args.tmp_updates_file, mode) as tmp_update_file:
    root = ET.fromstring(xml)

    records = root.findall('OAI:ListRecords/OAI:record', OAI_XML_NAMESPACES)
    for record in records:

      arXivRaw = record.find('OAI:metadata/arXiv:arXivRaw', OAI_XML_NAMESPACES)
      count_records = count_records + 1

      text_keys = [
          'id', 'submitter', 'authors', 'title', 'comments',
          'journal-ref', 'doi', 'report-no',
          'categories', 'license', 
          'abstract', 
      ]
      output = { key: _record_element_text(arXivRaw, key) for key in text_keys }

      output['versions'] = []
      for version in _record_element_all(arXivRaw, 'version'):
        h = {}
        h['version'] = version.attrib['version']
        h['created'] = _record_element_text(version, 'date')
        output['versions'].append(h)

      update_date = record.find('OAI:header/OAI:datestamp', OAI_XML_NAMESPACES).text
      if update_date:
        output['update_date'] = update_date

                                         # Derived
      output['authors_parsed'] = parse_author_affil_utf(output['authors'])

      print(json.dumps(output, separators=(',', ':')), 
            file=tmp_update_file)

  rt = root.find('OAI:ListRecords/OAI:resumptionToken', OAI_XML_NAMESPACES)
  next_resumption_token = rt.text if rt is not None else None

  return next_resumption_token, count_records


def parse_args():
  global args

  p = argparse.ArgumentParser()
  p.add_argument('-v', '--verbose',    dest='verbose',    action='count', default=0)
  p.add_argument('-n', '--dry-run',    dest='dry_run',    action='store_true')
  p.add_argument(      '--reload-all', dest='reload_all', action='store_true')
  p.add_argument('-f', '--from-days-ago',    default=FROM_DAYS_AGO, type=int)
  p.add_argument('-j', '--json-file',        default=f'{ F_JSON }')
  p.add_argument('-u', '--tmp-updates-file', default=f'{ F_TMP_UPDATES }')
  p.add_argument('-s', '--tmp-sorted-file',  default=f'{ F_TMP_SORTED }')
  p.add_argument('-m', '--tmp-merged-file',  default=f'{ F_TMP_MERGED }')
  p.add_argument('-t', '--resumption-token', default=None)
  args = p.parse_args()


def _record_element_all(elm, name):
  """ Extract text from multiple nodes """
  return elm.findall(f'arXiv:{ name }', OAI_XML_NAMESPACES) if elm is not None else []


def _record_element_text(elm, name):
  """ Extract text from leaf (single-node) elements """
  item = elm.find(f'arXiv:{ name }', OAI_XML_NAMESPACES) if elm is not None else None
  return item.text if item is not None else None


def sort_the_updates_file():
  result = os.system( f'sort -o {args.tmp_sorted_file} {args.tmp_updates_file}' )
  if result != 0:
    print(f'Issue sorting')

                                       # python -m unittest update_kaggle_metadata.py
class TestCompareMatchesUsrBinSort(unittest.TestCase):
  def test_sort_by_paper_id1(self):
    return "0704.0002" < "cs/1" == True


'''
=========================================================================
Code below copied from:
  https://github.com/arXiv/arxiv-base/blob/develop/arxiv/util/authors.py
  https://github.com/arXiv/arxiv-base/blob/develop/arxiv/util/tex2utf.py
========================================================================
'''

import json
import re
from itertools import dropwhile
from typing import Dict, Iterator, List, Tuple

PREFIX_MATCH = 'van|der|de|la|von|del|della|da|mac|ter|dem|di|vaziri'

"""
Takes data from an Author: line in the current arXiv abstract
file and returns a structured set of data:
 author_list_ptr = [
  [ author1_keyname, author1_firstnames, author1_suffix, affil1, affil2 ] ,
  [ author2_keyname, author2_firstnames, author1_suffix, affil1 ] ,
  [ author3_keyname, author3_firstnames, author1_suffix ]
         ]
Abstracted from Dienst software for OAI1 and other uses. This
routine should just go away when a better metadata structure is
adopted that deals with names and affiliations properly.
Must remember that there is at least one person one the archive
who has only one name, this should clearly be considered the key name.
Code originally written by Christina Scovel, Simeon Warner Dec99/Jan00
 2000-10-16 - separated.
 2000-12-07 - added support for suffix
 2003-02-14 - get surname prefixes from arXiv::Filters::Index [Simeon]
 2007-10-01 - created test script, some tidying [Simeon]
 2018-05-25 - Translated from Perl to Python [Brian C.]
"""


def parse_author_affil(authors: str) -> List[List[str]]:
    """
    Parse author line and returns an list of author and affiliation data.
    The list for each author will have at least three elements for
    keyname, firstname(s) and suffix. The keyname will always have content
    but the other strings might be empty strings if there is no firstname
    or suffix. Any additional elements after the first three are affiliations,
    there may be zero or more.
    Handling of prefix "XX collaboration" etc. is duplicated here and in
    arXiv::HTML::AuthorLink -- it shouldn't be. Likely should just be here.
    This routine is just a wrapper around the two parts that first split
    the authors line into parts, and then back propagate the affiliations.
    The first part is to be used along for display where we do not want
    to back propagate affiliation information.
    :param authors: string of authors from abs file or similar
    :return:
    Returns a structured set of data:
    author_list_ptr = [
       [ author1_keyname, author1_firstnames, author1_suffix, affil1, affil2 ],
       [ author2_keyname, author2_firstnames, author1_suffix, affil1 ] ,
       [ author3_keyname, author3_firstnames, author1_suffix ]
    ]
    """
    return _parse_author_affil_back_propagate(
        **_parse_author_affil_split(authors))


def _parse_author_affil_split(author_line: str) -> Dict:
    """
    Split author line into author and affiliation data.
    Take author line, tidy spacing and punctuation, and then split up into
    individual author an affiliation data. Has special cases to avoid splitting
    an initial collaboration name and records in $back_propagate_affiliation_to
    the fact that affiliations should not be back propagated to collaboration
    names.
    Does not handle multiple collaboration names.
    """
    if not author_line:
        return {'author_list': [], 'back_prop': 0}

    names: List[str] = split_authors(author_line)
    if not names:
        return {'author_list': [], 'back_prop': 0}

    names = _remove_double_commas(names)
    # get rid of commas at back
    namesIter: Iterator[str] = reversed(
        list(dropwhile(lambda x: x == ',', reversed(names))))
    # get rid of commas at front
    names = list(dropwhile(lambda x: x == ',', namesIter))

    # Extract all names (all parts not starting with comma or paren)
    names = list(map(_tidy_name, filter(
        lambda x: re.match('^[^](,]', x), names)))
    names = list(filter(lambda n: not re.match(
        r'^\s*et\.?\s+al\.?\s*', n, flags=re.IGNORECASE), names))

    (names, author_list,
     back_propagate_affiliations_to) = _collaboration_at_start(names)

    (enumaffils) = _enum_collaboration_at_end(author_line)

    # Split name into keyname and firstnames/initials.
    # Deal with different patterns in turn: prefixes, suffixes, plain
    # and single name.
    patterns = [('double-prefix',
                 r'^(.*)\s+(' + PREFIX_MATCH + r')\s(' +
                 PREFIX_MATCH + r')\s(\S+)$'),
                ('name-prefix-name',
                 r'^(.*)\s+(' + PREFIX_MATCH + r')\s(\S+)$'),
                ('name-name-prefix',
                 r'^(.*)\s+(\S+)\s(I|II|III|IV|V|Sr|Jr|Sr\.|Jr\.)$'),
                ('name-name',
                 r'^(.*)\s+(\S+)$'), ]

    # Now go through names in turn and try to get affiliations
    # to go with them
    for name in names:
        pattern_matches = ((mtype, re.match(m, name, flags=re.IGNORECASE))
                           for (mtype, m) in patterns)

        (mtype, match) = next(((mtype, m)
                               for (mtype, m) in pattern_matches
                               if m is not None), ('default', None))
        if match is None:
            author_entry = [name, '', '']
        elif mtype == 'double-prefix':
            s = '{} {} {}'.format(match.group(
                2), match.group(3), match.group(4))
            author_entry = [s, match.group(1), '']
        elif mtype == 'name-prefix-name':
            s = '{} {}'.format(match.group(2), match.group(3))
            author_entry = [s, match.group(1), '']
        elif mtype == 'name-name-prefix':
            author_entry = [match.group(2), match.group(1), match.group(3)]
        elif mtype == 'name-name':
            author_entry = [match.group(2), match.group(1), '']
        else:
            author_entry = [name, '', '']

        # search back in author_line for affiliation
        author_entry = _add_affiliation(
            author_line, enumaffils, author_entry, name)
        author_list.append(author_entry)

    return {'author_list': author_list,
            'back_prop': back_propagate_affiliations_to}


def parse_author_affil_utf(authors: str) -> List:
    """
    Call parse_author_affil() and do TeX to UTF conversion.
    Output structure is the same but should be in UTF and not TeX.
    """
    if not authors:
        return []
    return list(map(lambda author: list(map(tex2utf, author)),
                    parse_author_affil(authors)))


def _remove_double_commas(items: List[str]) -> List[str]:

    parts: List[str] = []
    last = ''
    for pt in items:
        if pt == ',' and last == ',':
            continue
        else:
            parts.append(pt)
            last = pt
    return parts


def _tidy_name(name: str) -> str:
    name = re.sub(r'\s\s+', ' ', name)  # also gets rid of CR
    # add space after dot (except in TeX)
    name = re.sub(r'(?<!\\)\.(\S)', r'. \g<1>', name)
    return name


def _collaboration_at_start(names: List[str]) \
        -> Tuple[List[str], List[List[str]], int]:
    """Perform special handling of collaboration at start."""
    author_list = []

    back_propagate_affiliations_to = 0
    while len(names) > 0:
        m = re.search(r'([a-z0-9\s]+\s+(collaboration|group|team))',
                      names[0], flags=re.IGNORECASE)
        if not m:
            break

        # Add to author list
        author_list.append([m.group(1), '', ''])
        back_propagate_affiliations_to += 1
        # Remove from names
        names.pop(0)
        # Also swallow and following comma or colon
        if names and (names[0] == ',' or names[0] == ':'):
            names.pop(0)

    return names, author_list, back_propagate_affiliations_to


def _enum_collaboration_at_end(author_line: str)->Dict:
    """Get separate set of enumerated affiliations from end of author_line."""
    # Now see if we have a separate set of enumerated affiliations
    # This is indicated by finding '(\s*('
    line_m = re.search(r'\(\s*\((.*)$', author_line)
    if not line_m:
        return {}

    enumaffils = {}
    affils = re.sub(r'\s*\)\s*$', '', line_m.group(1))

    # Now expect to have '1) affil1 (2) affil2 (3) affil3'
    for affil in affils.split('('):
        # Now expect `1) affil1 ', discard if no match
        m = re.match(r'^(\d+)\)\s*(\S.*\S)\s*$', affil)
        if m:
            enumaffils[m.group(1)] = re.sub(r'[\.,\s]*$', '', m.group(2))

    return enumaffils


def _add_affiliation(author_line: str,
                     enumaffils: Dict,
                     author_entry: List[str],
                     name: str) -> List:
    """
    Add author affiliation to author_entry if one is found in author_line.
    This should deal with these cases
    Smith B(labX) Smith B(1) Smith B(1, 2) Smith B(1 & 2) Smith B(1 and 2)
    """
    en = re.escape(name)
    namerex = r'{}\s*\(([^\(\)]+)'.format(en.replace(' ', 's*'))
    m = re.search(namerex, author_line, flags=re.IGNORECASE)
    if not m:
        return author_entry

    # Now see if we have enumerated references (just commas, digits, &, and)
    affils = m.group(1).rstrip().lstrip()
    affils = re.sub(r'(&|and)/,', ',', affils, flags=re.IGNORECASE)

    if re.match(r'^[\d,\s]+$', affils):
        for affil in affils.split(','):
            if affil in enumaffils:
                author_entry.append(enumaffils[affil])
    else:
        author_entry.append(affils)

    return author_entry


def _parse_author_affil_back_propagate(author_list: List[List[str]],
                                       back_prop: int) -> List[List[str]]:
    """Back propagate author affiliation.
    Take the author list structure generated by parse_author_affil_split(..)
    and propagate affiliation information backwards to preceeding author
    entries where none was give. Stop before entry $back_prop to avoid
    adding affiliation information to collaboration names.
    given, eg:
      a.b.first, c.d.second (affil)
    implies
      a.b.first (affil), c.d.second (affil)
    and in more complex cases:
      a.b.first, c.d.second (1), e.f.third, g.h.forth (2,3)
    implies
      a.b.first (1), c.d.second (1), e.f.third (2,3), g.h.forth (2,3)
    """
    last_affil: List[str] = []
    for x in range(len(author_list) - 1, max(back_prop - 1, -1), -1):
        author_entry = author_list[x]
        if len(author_entry) > 3:  # author has affiliation,store
            last_affil = author_entry
        elif last_affil:
            # author doesn't have affil but later one did => copy
            author_entry.extend(last_affil[3:])

    return author_list


def split_authors(authors: str) -> List:
    """
    Split author string into authors entity lists.
    Take an author line as a string and return a reference to a list of the
    different name and affiliation blocks. While this does normalize spacing
    and 'and', it is a key feature that the set of strings returned can be
    concatenated to reproduce the original authors line. This code thus
    provides a very graceful degredation for badly formatted authors lines, as
    the text at least shows up.
    """
    # split authors field into blocks with boundaries of ( and )
    if not authors:
        return []
    aus = re.split(r'(\(|\))', authors)
    aus = list(filter(lambda x: x != '', aus))

    blocks = []
    if len(aus) == 1:
        blocks.append(authors)
    else:
        c = ''
        depth = 0
        for bit in aus:
            if bit == '':
                continue
            if bit == '(':  # track open parentheses
                depth += 1
                if depth == 1:
                    blocks.append(c)
                    c = '('
                else:
                    c = c + bit
            elif bit == ')':  # track close parentheses
                depth -= 1
                c = c + bit
                if depth == 0:
                    blocks.append(c)
                    c = ''
                else:  # haven't closed, so keep accumulating
                    continue
            else:
                c = c + bit
        if c:
            blocks.append(c)

    listx = []

    for block in blocks:
        block = re.sub(r'\s+', ' ', block)
        if re.match(r'^\(', block):  # it is a comment
            listx.append(block)
        else:  # it is a name
            block = re.sub(r',?\s+(and|\&)\s', ',', block)
            names = re.split(r'(,|:)\s*', block)
            for name in names:
                if not name:
                    continue
                name = name.rstrip().lstrip()
                if name:
                    listx.append(name)

    # Recombine suffixes that were separated with a comma
    parts: List[str] = []
    for p in listx:
        if re.match(r'^(Jr\.?|Sr\.?\[IV]{2,})$', p) \
                and len(parts) >= 2 \
                and parts[-1] == ',' \
                and not re.match(r'\)$', parts[-2]):
            separator = parts.pop()
            last = parts.pop()
            recomb = "{}{} {}".format(last, separator, p)
            parts.append(recomb)
        else:
            parts.append(p)

    return parts

''' End of: authors.py '''
''' Start of: tex2utf.py '''


''' End of: tex2utf.py '''

"""Convert between TeX escapes and UTF8."""
import re
from typing import Pattern, Dict, Match

accents = {
    # first accents with non-letter prefix, e.g. \'A
    "'A": 0x00c1, "'C": 0x0106, "'E": 0x00c9, "'I": 0x00cd,
    "'L": 0x0139, "'N": 0x0143, "'O": 0x00d3, "'R": 0x0154,
    "'S": 0x015a, "'U": 0x00da, "'Y": 0x00dd, "'Z": 0x0179,
    "'a": 0x00e1, "'c": 0x0107, "'e": 0x00e9, "'i": 0x00ed,
    "'l": 0x013a, "'n": 0x0144, "'o": 0x00f3, "'r": 0x0155,
    "'s": 0x015b, "'u": 0x00fa, "'y": 0x00fd, "'z": 0x017a,
    '"A': 0x00c4, '"E': 0x00cb, '"I': 0x00cf, '"O': 0x00d6,
    '"U': 0x00dc, '"Y': 0x0178, '"a': 0x00e4, '"e': 0x00eb,
    '"i': 0x00ef, '"o': 0x00f6, '"u': 0x00fc, '"y': 0x00ff,
    '.A': 0x0226, '.C': 0x010a, '.E': 0x0116, '.G': 0x0120,
    '.I': 0x0130, '.O': 0x022e, '.Z': 0x017b, '.a': 0x0227,
    '.c': 0x010b, '.e': 0x0117, '.g': 0x0121, '.o': 0x022f,
    '.z': 0x017c, '=A': 0x0100, '=E': 0x0112, '=I': 0x012a,
    '=O': 0x014c, '=U': 0x016a, '=Y': 0x0232, '=a': 0x0101,
    '=e': 0x0113, '=i': 0x012b, '=o': 0x014d, '=u': 0x016b,
    '=y': 0x0233, '^A': 0x00c2, '^C': 0x0108, '^E': 0x00ca,
    '^G': 0x011c, '^H': 0x0124, '^I': 0x00ce, '^J': 0x0134,
    '^O': 0x00d4, '^S': 0x015c, '^U': 0x00db, '^W': 0x0174,
    '^Y': 0x0176, '^a': 0x00e2, '^c': 0x0109, '^e': 0x00ea,
    '^g': 0x011d, '^h': 0x0125, '^i': 0x00ee, '^j': 0x0135,
    '^o': 0x00f4, '^s': 0x015d, '^u': 0x00fb, '^w': 0x0175,
    '^y': 0x0177, '`A': 0x00c0, '`E': 0x00c8, '`I': 0x00cc,
    '`O': 0x00d2, '`U': 0x00d9, '`a': 0x00e0, '`e': 0x00e8,
    '`i': 0x00ec, '`o': 0x00f2, '`u': 0x00f9, '~A': 0x00c3,
    '~I': 0x0128, '~N': 0x00d1, '~O': 0x00d5, '~U': 0x0168,
    '~a': 0x00e3, '~i': 0x0129, '~n': 0x00f1, '~o': 0x00f5,
    '~u': 0x0169,
    # and now ones with letter prefix \c{c} etc..
    'HO': 0x0150, 'HU': 0x0170, 'Ho': 0x0151, 'Hu': 0x0171,
    'cC': 0x00c7, 'cE': 0x0228,
    'cG': 0x0122, 'cK': 0x0136, 'cL': 0x013b, 'cN': 0x0145,
    'cR': 0x0156, 'cS': 0x015e, 'cT': 0x0162, 'cc': 0x00e7,
    'ce': 0x0229, 'cg': 0x0123, 'ck': 0x0137, 'cl': 0x013c,
    # Commented out due ARXIVDEV-2322 (bug reported by PG)
    # 'ci' : 'i\x{0327}' = chr(0x69).ch(0x327) # i with combining cedilla
    'cn': 0x0146, 'cr': 0x0157, 'cs': 0x015f, 'ct': 0x0163,
    'kA': 0x0104, 'kE': 0x0118, 'kI': 0x012e, 'kO': 0x01ea,
    'kU': 0x0172, 'ka': 0x0105, 'ke': 0x0119, 'ki': 0x012f,
    'ko': 0x01eb, 'ku': 0x0173, 'rA': 0x00c5, 'rU': 0x016e,
    'ra': 0x00e5, 'ru': 0x016f, 'uA': 0x0102, 'uE': 0x0114,
    'uG': 0x011e, 'uI': 0x012c, 'uO': 0x014e, 'uU': 0x016c,
    'ua': 0x0103, 'ue': 0x0115, 'ug': 0x011f,
    'ui': 0x012d, 'uo': 0x014f, 'uu': 0x016d,
    'vA': 0x01cd, 'vC': 0x010c, 'vD': 0x010e,
    'vE': 0x011a, 'vG': 0x01e6, 'vH': 0x021e, 'vI': 0x01cf,
    'vK': 0x01e8, 'vL': 0x013d, 'vN': 0x0147, 'vO': 0x01d1,
    'vR': 0x0158, 'vS': 0x0160, 'vT': 0x0164, 'vU': 0x01d3,
    'vZ': 0x017d, 'va': 0x01ce, 'vc': 0x010d, 'vd': 0x010f,
    've': 0x011b, 'vg': 0x01e7, 'vh': 0x021f, 'vi': 0x01d0,
    'vk': 0x01e9, 'vl': 0x013e, 'vn': 0x0148, 'vo': 0x01d2,
    'vr': 0x0159, 'vs': 0x0161, 'vt': 0x0165, 'vu': 0x01d4,
    'vz': 0x017e
}
r"""
Hash to lookup tex markup and convert to Unicode.
macron: a line above character (overbar \={} in TeX)
caron: v-shape above character (\v{ } in TeX)
See: http://www.unicode.org/charts/
"""

textlet = {
    'AA': 0x00c5, 'AE': 0x00c6, 'DH': 0x00d0, 'DJ': 0x0110,
    'ETH': 0x00d0, 'L': 0x0141, 'NG': 0x014a, 'O': 0x00d8,
    'oe': 0x0153, 'OE': 0x0152, 'TH': 0x00de, 'aa': 0x00e5,
    'ae': 0x00e6,
    'dh': 0x00f0, 'dj': 0x0111, 'eth': 0x00f0, 'i': 0x0131,
    'l': 0x0142, 'ng': 0x014b, 'o': 0x00f8, 'ss': 0x00df,
    'th': 0x00fe,
    }

textgreek = {
    # Greek (upper)
    'Gamma': 0x0393, 'Delta': 0x0394, 'Theta': 0x0398,
    'Lambda': 0x039b, 'Xi': 0x039E, 'Pi': 0x03a0,
    'Sigma': 0x03a3, 'Upsilon': 0x03a5, 'Phi': 0x03a6,
    'Psi': 0x03a8, 'Omega': 0x03a9,
    # Greek (lower)
    'alpha': 0x03b1, 'beta': 0x03b2, 'gamma': 0x03b3,
    'delta': 0x03b4, 'epsilon': 0x03b5, 'zeta': 0x03b6,
    'eta': 0x03b7, 'theta': 0x03b8, 'iota': 0x03b9,
    'kappa': 0x03ba, 'lambda': 0x03bb, 'mu': 0x03bc,
    'nu': 0x03bd, 'xi': 0x03be, 'omicron': 0x03bf,
    'pi': 0x03c0, 'rho': 0x03c1, 'varsigma': 0x03c2,
    'sigma': 0x03c3, 'tau': 0x03c4, 'upsion': 0x03c5,
    'varphi': 0x03C6,  # φ
    'phi':  0x03D5,  # ϕ
    'chi': 0x03c7, 'psi': 0x03c8, 'omega': 0x03c9,
}


def _p_to_match(tex_to_chr: Dict[str, int]) -> Pattern:
    # textsym and textlet both use the same sort of regex pattern.
    keys = r'\\(' + '|'.join(tex_to_chr.keys()) + ')'
    pstr = r'({)?' + keys + r'(\b|(?=_))(?(1)}|(\\(?= )| |{}|)?)'
    return re.compile(pstr)


textlet_pattern = _p_to_match(textlet)
textgreek_pattern = _p_to_match(textgreek)

textsym = {
    'P': 0x00b6, 'S': 0x00a7, 'copyright': 0x00a9,
    'guillemotleft': 0x00ab, 'guillemotright': 0x00bb,
    'pounds': 0x00a3, 'dag': 0x2020, 'ddag': 0x2021,
    'div': 0x00f7, 'deg': 0x00b0}

textsym_pattern = _p_to_match(textsym)


def _textlet_sub(match: Match) -> str:
    return chr(textlet[match.group(2)])


def _textsym_sub(match: Match) -> str:
    return chr(textsym[match.group(2)])


def _textgreek_sub(match: Match) -> str:
    return chr(textgreek[match.group(2)])


def texch2UTF(acc: str) -> str:
    """Convert single character TeX accents to UTF-8.
    Strip non-whitepsace characters from any sequence not recognized (hence
    could return an empty string if there are no word characters in the input
    string).
    chr(num) will automatically create a UTF8 string for big num
    """
    if acc in accents:
        return chr(accents[acc])
    else:
        return re.sub(r'[^\w]+', '', acc, flags=re.IGNORECASE)


def tex2utf(tex: str, greek: bool = True) -> str:
    r"""Convert some TeX accents and greek symbols to UTF-8 characters.
    :param tex: Text to filter.
    :param greek: If False, do not convert greek letters or
    ligatures.  Greek symbols can cause problems. Ex. \phi is not
    suppose to look like φ. φ looks like \varphi.  See ARXIVNG-1612
    :returns: string, possibly with some TeX replaced with UTF8
    """
    # Do dotless i,j -> plain i,j where they are part of an accented i or j
    utf = re.sub(r"/(\\['`\^\"\~\=\.uvH])\{\\([ij])\}", r"\g<1>\{\g<2>\}", tex)

    # Now work on the Tex sequences, first those with letters only match
    utf = textlet_pattern.sub(_textlet_sub, utf)

    if greek:
        utf = textgreek_pattern.sub(_textgreek_sub, utf)

    utf = textsym_pattern.sub(_textsym_sub, utf)

    utf = re.sub(r'\{\\j\}|\\j\s', 'j', utf)  # not in Unicode?

    # reduce {{x}}, {{{x}}}, ... down to {x}
    while re.search(r'\{\{([^\}]*)\}\}', utf):
        utf = re.sub(r'\{\{([^\}]*)\}\}', r'{\g<1>}', utf)

    # Accents which have a non-letter prefix in TeX, first \'e
    utf = re.sub(r'\\([\'`^"~=.][a-zA-Z])',
                 lambda m: texch2UTF(m.group(1)), utf)

    # then \'{e} form:
    utf = re.sub(r'\\([\'`^"~=.])\{([a-zA-Z])\}',
                 lambda m: texch2UTF(m.group(1) + m.group(2)), utf)

    # Accents which have a letter prefix in TeX
    #  \u{x} u above (breve), \v{x}   v above (caron), \H{x}   double accute...
    utf = re.sub(r'\\([Hckoruv])\{([a-zA-Z])\}',
                 lambda m: texch2UTF(m.group(1) + m.group(2)), utf)

    # Don't do \t{oo} yet,
    utf = re.sub(r'\\t{([^\}])\}', r'\g<1>', utf)

    # bdc34: commented out in original Perl
    # $utf =~ s/\{(.)\}/$1/g; #  remove { } from around {x}

    return utf


if __name__ == "__main__":
  main()