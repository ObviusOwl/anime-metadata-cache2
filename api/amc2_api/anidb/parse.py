import dataclasses
import datetime
import logging
import io
import copy

import xml.sax
import xml.sax.xmlreader
import xml.etree.ElementTree as ET

from amc2_api.model import Title, Episode, Anime, Image, ImageType, CastRole, Rating, Credit, Season
from amc2_api.utils import URL

from typing import List, Optional, Callable, Dict, Union, Tuple, Callable

_logger = logging.getLogger(__name__)


class TitleXMLHandler(xml.sax.ContentHandler):
    title_handler: Callable[[Title], None]

    _title: Dict[str, str]
    _aid: str

    def __init__(self, title_handler: Callable[[Title], None]):
        self.title_handler = title_handler
        self._title = {}
        self._aid = ''
    
    def _translate_type(self, value: str) -> str:
        if value == 'syn':
            return 'synonym'
        return value

    def _translate_lang(self, value: str) -> str:
        return value

    def startElement(self, tag: str, attributes: xml.sax.xmlreader.AttributesImpl):
        if tag.lower() == 'anime':
            self._aid = attributes['aid']
        elif tag.lower() == 'title':
            self._title = {
                'type': self._translate_type(attributes['type']),
                'lang': self._translate_lang(attributes['xml:lang']),
                'value': ''
            }

    def endElement(self, tag: str) -> None:
        if tag.lower() == 'title':
            self._title['aid'] = self._aid
            title = Title.construct(**self._title)
            self.title_handler(title)
            self._title = {}

    def characters(self, content: Union[str, bytes]) -> None:
        if isinstance(content, bytes):
            content = content.decode()
        if 'value' in self._title:
            self._title['value'] += content


def parse_titles_xml(data: str, title_handler: Callable[[Title], None]) -> None:
    parser = xml.sax.make_parser()
    handler = TitleXMLHandler(title_handler)
    parser.setContentHandler(handler)

    source = xml.sax.InputSource()
    source.setCharacterStream(io.StringIO(data))
    parser.parse(source)


def parse_api_error(data: bytes) -> str:
    try:
        root = ET.fromstring(data.decode())
    except Exception:
        return ''
    
    if root.tag.lower() != 'error':
        return ''
    return root.text.lower()


@dataclasses.dataclass
class AnidbTag:
    id: str
    name: str
    parent: Optional['AnidbTag'] = None

    @staticmethod
    def parse_tree(elements: List[ET.Element]) -> List['AnidbTag']:
        all_tags: Dict[str, AnidbTag] = {}
        parents: Dict[str, str] = {}
        for el in elements:
            tag, parent_id = AnidbTag.parse(el)
            if tag.name:
                all_tags[tag.id] = tag
                parents[tag.id] = parent_id
        
        for tag in all_tags.values():
            tag.parent = all_tags.get(parents[tag.id], None)

        parent_ids = set(parents.values())
        return [all_tags[k] for k in all_tags.keys() if k not in parent_ids]

    @classmethod
    def parse(self, el: ET.Element) -> Tuple['AnidbTag', str]:
        name: str = ''
        if (name_el := el.find('./name')) is not None:
            name = name_el.text.strip()
        parent_id = el.attrib.get('parentid', '').strip()
        return AnidbTag(id=el.attrib['id'], name=name), parent_id

    def path(self) -> List['AnidbTag']:
        path: List[AnidbTag] = []
        node: Optional[AnidbTag] = self
        while node is not None:
            path.append(node)
            node = node.parent
        return list(reversed(path))


class AnimeXMLParser:
    """
    The numerical enums are described in the UDP api instead of the HTTP api
    
    https://wiki.anidb.net/UDP_API_Definition

    Special episodes are filtered out.
    """

    @classmethod
    def parse(cls, xml_data: str) -> Anime:
        try:
            root = ET.fromstring(xml_data)
            return cls().parse_anime(root)
        except ET.ParseError as e:
            raise ValueError("Invalid XML: " + str(e))
    
    def _parse_str(self, el: Optional[ET.Element], default: str) -> str:
        return el.text if el is not None else default

    def _parse_int(self, el: Optional[ET.Element], default: int) -> int:
        if el is None or not el.text:
            return default
        return int(el.text)
    
    def _parse_date(self, el: Optional[ET.Element], default: str) -> datetime.date:
        if el is None or not el.text:
            return datetime.date.fromisoformat(default)
        return datetime.date.fromisoformat(el.text)

    def _parse_creators(self, creators_el: ET.Element, ctype: str) -> List[str]:
        ctype = ctype.lower()

        creators: List[str] = []
        for el in creators_el.findall('./name'):
            if el.attrib.get('type', '').lower() == ctype and el.text.strip():
                creators.append(el.text.strip())
        return creators

    def _parse_creators_credits(self, name_el: ET.Element) -> List[Credit]:
        # Mapping from anidb jobs to tmdb department/known_for_department

        cat_map = {
            "Character Design": "visual effects",
            "Original Work": "writing",
            "Music": "sound",
            "Animation Work": "visual effects",
            "Direction": "directing",
            "Chief Animation Direction": "directing",
            "Animation Character Design": "visual effects",
            "Series Composition": "writing",
        }

        dep_map = {
            "Character Design": "Art",
            "Original Work": "Writing",
            "Music": "Sound",
            "Animation Work": "Art",
            "Direction": "Directing",
            "Chief Animation Direction": "Directing",
            "Animation Character Design": "Art",
            "Series Composition": "Writing",
        }

        credits: List[Credit] = []
        for el in name_el:
            name = el.text.strip()
            job = el.attrib.get('type', '').strip()

            if job and name:
                credit = Credit(
                    name=name,
                    job=job,
                    department=dep_map.get(job, ''),
                    category=cat_map.get(job, ''),
                )
                credits.append(credit)
        return credits

    def _parse_tags(self, elements: List[ET.Element]) -> List[str]:
        # we keep only the leaf tags with are not maintenance tags
        # the inner nodes of the tag tree are more like tag-categories
        tag_names: List[str] = []
        for tag in AnidbTag.parse_tree(elements):
            path = [t.name.lower() for t in tag.path()]
            if not 'maintenance tags' in path:
                tag_names.append(tag.name)
        return tag_names

    def parse_image(self, el: ET.Element) -> Image:
        name = el.text.strip().strip('/')
        # anidb only has posters
        return Image(type=ImageType.poster, name=name, source='anidb')

    def parse_title(self, el: ET.Element) -> Title:
        lang = el.attrib.get('{http://www.w3.org/XML/1998/namespace}lang', '')
        typ = el.attrib.get('type', '')
        if typ == 'syn':
            typ = 'synonym'
        return Title(lang=lang, type=typ, value=el.text)
    
    def parse_epno(self, el: ET.Element) -> Tuple[int, int]:
        """
        https://wiki.anidb.net/UDP_API_Definition

        Returned 'epno' includes special character (only if special) and padding (only if normal).
        Special characters are S(special), C(credits), T(trailer), P(parody), O(other).

        The type is the raw episode type, used to indicate numerically what the special character will be
        1: regular episode (no prefix), 2: special ("S"), 3: credit ("C"), 4: trailer ("T"), 5: parody ("P"), 6: other ("O")
        """
        typ = int(el.attrib['type'])
        if typ == 1:
            epno = int(el.text.strip())
        else:
            epno = int(el.text.strip()[1:])
        return typ, epno
    
    def parse_rating(self, el: Optional[ET.Element], votes_attr: str = 'votes') -> Optional[Rating]:
        if el is None:
            return None

        try:
            average = float(el.text)
        except ValueError:
            return None

        try:
            votes = int(el.attrib.get(votes_attr, '0'))
        except ValueError:
            votes = 0
    
        return Rating(source='anidb', average=average, votes=votes)
    
    def parse_episode(self, el: ET.Element, specials: bool = False) -> Optional[Episode]:
        ep_type, ep_no = self.parse_epno(el.find('./epno'))
        if (not specials and ep_type != 1) or (specials and ep_type != 2):
            return None

        length = self._parse_int(el.find('./length'), 0)
        airdate = self._parse_date(el.find('./airdate'), '0001-01-01')
        summary = self._parse_str(el.find('./summary'), '').strip()
        titles = [self.parse_title(t) for t in el.findall('./title')]
        rating = self.parse_rating(el.find('./rating'))

        return Episode(
            number=ep_no,
            length=length,
            airdate=airdate,
            titles=titles,
            summary=summary,
            images=[],
            ratings=[rating] if rating else [],
        )
    
    def parse_character(self, el: ET.Element) -> Optional[CastRole]:
        # note: seiyuu is the voice actor of an anime character
        seiyuu = el.find('./seiyuu')
        if seiyuu is None:
            # don't return characters without cast
            return None

        char_img_el = el.find('./picture')
        if char_img_el is not None:
            char_img = char_img_el.text.strip().strip('/')
        else:
            char_img = ''

        seiyuu_img = seiyuu.attrib.get('picture', '').strip().strip('/')
        
        role = CastRole(
            character=self._parse_str(el.find('./name'), '').strip(),
            actor=seiyuu.text.strip(),
        )

        if char_img:
            role.character_image = Image(type=ImageType.thumb, name=char_img, source='anidb')
        if seiyuu_img:
            role.actor_image = Image(type=ImageType.thumb, name=seiyuu_img, source='anidb')
        return role

    def _parse_season(self, anime: Anime, el: ET.Element) -> Season:
        eps = [self.parse_episode(e, False) for e in el.findall('./episodes/episode')]
        eps = [e for e in eps if e is not None]

        return Season(
            id=anime.id, 
            number=1,
            uniqueids=copy.deepcopy(anime.uniqueids),
            titles=copy.deepcopy(anime.titles), 

            description=copy.deepcopy(anime.description),
            genres=copy.deepcopy(anime.genres), 
            tags=copy.deepcopy(anime.tags),
            airdate=copy.deepcopy(anime.airdate),
            episodes=eps,
            images=copy.deepcopy(anime.images),
            ratings=copy.deepcopy(anime.ratings),

            cast=copy.deepcopy(anime.cast),
            directors=copy.deepcopy(anime.directors),
            credits=copy.deepcopy(anime.credits),
        )
    
    def _parse_specials(self, anime: Anime, el: ET.Element) -> Season:
        eps = [self.parse_episode(e, True) for e in el.findall('./episodes/episode')]
        eps = [e for e in eps if e is not None]

        return Season(
            id=anime.id, 
            number=0,
            uniqueids=copy.deepcopy(anime.uniqueids),
            titles=[Title(value='Specials', type='main', lang='en')], 
            episodes=eps,
        )

    def parse_anime(self, el: ET.Element) -> Anime:
        aid = el.attrib['id']
        descr = self._parse_str(el.find('./description'), '').strip()
        imgs = [self.parse_image(e) for e in el.findall('./picture')]

        titles = [self.parse_title(t) for t in el.findall('./titles/title')]
        for title in titles:
            title.aid = aid

        eps = [self.parse_episode(e) for e in el.findall('./episodes/episode')]
        eps = [e for e in eps if e is not None]

        chars = [self.parse_character(c) for c in el.findall('./characters/character')]
        chars = [c for c in chars if c is not None]

        airdate: Optional[datetime.date] = None
        if (sdate_el := el.find('./startdate')) is not None and sdate_el.text:
            airdate = self._parse_date(sdate_el, '0001-01-01')

        creators_el = el.find('./creators')
        directors = self._parse_creators(creators_el, 'Direction')
        
        rating: Optional[Rating] = None
        if (rating_el := el.find('./ratings/permanent')) is not None:
            rating = self.parse_rating(rating_el, votes_attr='count')

        tags = self._parse_tags(el.findall('./tags/tag'))

        credits = self._parse_creators_credits(el.findall('./creators/name'))

        anime = Anime(
            id='A' + aid, 
            titles=titles, 
            description=descr, 
            tags=tags,
            airdate=airdate,
            #episodes=eps,
            images=imgs,
            uniqueids={'anidb': aid},
            cast=chars,
            directors=directors,
            ratings=[rating] if rating else [],
            credits=credits,
        )

        anime.seasons = [
            self._parse_specials(anime, el),
            self._parse_season(anime, el)
        ]

        return anime
