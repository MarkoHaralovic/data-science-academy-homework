CREATE DICTIONARY mharalovic.event_dictionary
(
    id            Int32,
    sport_id      Int32,
    tournament_id Int32,
    season_id     Nullable(Int32),
    venue_id      Nullable(Int32),
    referee_id    Nullable(Int32),
    usercount     Int32,
    attendance    Nullable(Int32),
    startdate     DateTime64(6),
    hometeam_id   Int32,
    awayteam_id   Int32
)
PRIMARY KEY id
SOURCE (CLICKHOUSE(
        DATABASE 'sports'
        TABLE 'event'
        USER 'mharalovic'
        PASSWORD 'Fs75EePJ3m54EyysB75U'
))
LAYOUT (HASHED()) -- chosen due to neary 1000000 unique event ids
LIFETIME(MIN 0 MAX 1000);

CREATE DICTIONARY  mharalovic.sport_dictionary
(
    id   String,
    name String,
    slug String,
    external_id Nullable(Int32)
)
PRIMARY KEY id
SOURCE (
        CLICKHOUSE(
        DATABASE 'sports'
        TABLE 'sport'
        USER 'mharalovic'
        PASSWORD 'Fs75EePJ3m54EyysB75U'
        )
)
LAYOUT (FLAT()) -- chosen due to small number (32) of different ids
LIFETIME(MIN 0 MAX 1000);

CREATE DICTIONARY  mharalovic.tournament_dictionary
(
    id           Int32,
    category_id  Int32,
    name         String,
    slug         String,
    priority     Int32,
    order        Int32,
    visible      UInt8,
    startdate Nullable(DateTime64(6)),
    enddate Nullable(DateTime64(6)),
    externalid Nullable(Int32),
    externaltype Int16,
    uniquetournament_id Nullable(Int32),
    seasons      Array(Int32),
    extra Nullable(String),
    logo_id Nullable(Int32),
    isgroup Nullable(UInt8),
    groupname Nullable(String),
    roundprefix Nullable(String),
    disabled     UInt8,
    competitiontype Nullable(Int16),
    primarycolorhex Nullable(String),
    secondarycolorhex Nullable(String),
    firstcupround Nullable(Int32),
    coveredbyeditorstatistics Nullable(UInt8),
    periodlength_normaltime Nullable(Int16),
    periodlength_overtime Nullable(Int16),
    darklogo_id Nullable(Int32)
)

PRIMARY KEY id
SOURCE (CLICKHOUSE(
        DATABASE 'sports'
        TABLE 'tournament'
        USER 'mharalovic'
        PASSWORD 'Fs75EePJ3m54EyysB75U'
))
LAYOUT (HASHED()) -- chosen due to 90000 unique eids
LIFETIME(MIN 0 MAX 1000);

CREATE DICTIONARY  mharalovic.uniquetournament_dictionary
(
     id                         Int32,
    name                       String,
    slug                       String,
    priority                   Int32,
    order                      Int32,
    externalid Nullable(Int32),
    externaltype               Int16,
    extra Nullable(String),
    logo_id Nullable(Int32),
    category_id                Int32,
    usercount                  Int32,
    visible                    UInt8,
    title_holder_id Nullable(Int32),
    startdate Nullable(DateTime64(6)),
    enddate Nullable(DateTime64(6)),
    tier Nullable(Int32),
    titleholdertitles Nullable(Int32),
    mosttitles Nullable(Int32),
    groundtype Nullable(String),
    numberofsets Nullable(Int32),
    tennispoints Nullable(Int32),
    hasstandingsgroups Nullable(UInt8),
    hasrounds Nullable(UInt8),
    hasgroups Nullable(UInt8),
    hasplayoffseries Nullable(UInt8),
    hasdisabledhomeawaystandings Nullable(UInt8),
    manuallychecked Nullable(UInt8),
    primarycolorhex Nullable(String),
    secondarycolorhex Nullable(String),
    shortname Nullable(String),
    yearoffoundation Nullable(Int32),
    chairman Nullable(String),
    owner Nullable(String),
    numberofcompetitors Nullable(Int32),
    numberofdivisions Nullable(Int32),
    country_alpha2 Nullable(String),
    hasperformancegraphfeature UInt8,
    hasperformancegraph        UInt8,
    featuredeventtier Nullable(Int16),
    darklogo_id Nullable(Int32),
    crowdsourcingenabled       UInt8,
    periodlength_normaltime Nullable(Int32),
    periodlength_overtime Nullable(Int32),
    gender Nullable(String),
    haswinprobability Nullable(UInt8),
    standingssettings_id Nullable(Int32),
    lineupsmanualcreationrequired Nullable(UInt8)
)
PRIMARY KEY id
SOURCE (CLICKHOUSE(
        DATABASE 'sports'
        TABLE 'uniquetournament'
        USER 'mharalovic'
        PASSWORD 'Fs75EePJ3m54EyysB75U'
))
LAYOUT (FLAT()) -- chosen due to 9920 unique ids
LIFETIME(MIN 0 MAX 1000);

CREATE DICTIONARY mharalovic.team_dictionary
(
    id           Int32,
    sport_id     Int32,
    category_id  Int32,
    tournament_id Nullable(Int32),
    name         String,
    slug         String,
    shortname Nullable(String),
    gender Nullable(String),
    usercount    Int32,
    externalid Nullable(Int32),
    externaltype Int16,
    homecolor_hstripes Nullable(Int32),
    homecolor_main Nullable(Int32),
    homecolor_number Nullable(Int32),
    homecolor_sleeve Nullable(Int32),
    homecolor_split Nullable(Int32),
    homecolor_squares Nullable(Int32),
    homecolor_vstripes Nullable(Int32),
    awaycolor_hstripes Nullable(Int32),
    awaycolor_main Nullable(Int32),
    awaycolor_number Nullable(Int32),
    awaycolor_sleeve Nullable(Int32),
    awaycolor_split Nullable(Int32),
    awaycolor_squares Nullable(Int32),
    awaycolor_vstripes Nullable(Int32),
    homegoalkeepercolor_hstripes Nullable(Int32),
    homegoalkeepercolor_main Nullable(Int32),
    homegoalkeepercolor_number Nullable(Int32),
    homegoalkeepercolor_sleeve Nullable(Int32),
    homegoalkeepercolor_split Nullable(Int32),
    homegoalkeepercolor_squares Nullable(Int32),
    homegoalkeepercolor_vstripes Nullable(Int32),
    awaygoalkeepercolor_hstripes Nullable(Int32),
    awaygoalkeepercolor_main Nullable(Int32),
    awaygoalkeepercolor_number Nullable(Int32),
    awaygoalkeepercolor_sleeve Nullable(Int32),
    awaygoalkeepercolor_split Nullable(Int32),
    awaygoalkeepercolor_squares Nullable(Int32),
    awaygoalkeepercolor_vstripes Nullable(Int32),
    subteam1_id Nullable(Int32),
    subteam2_id Nullable(Int32),
    manager_id Nullable(Int32),
    logo_id Nullable(Int32),
    venue_id Nullable(Int32),
    extra Nullable(String),
    foundationdate Nullable(DateTime64(6)),
    homecolor_outline Nullable(Int32),
    awaycolor_outline Nullable(Int32),
    homegoalkeepercolor_outline Nullable(Int32),
    awaygoalkeepercolor_outline Nullable(Int32),
    namecode Nullable(String),
    ranking Nullable(Int32),
    class Nullable(Int32),
    primary_color Nullable(Int32),
    secondary_color Nullable(Int32),
    disabled Nullable(UInt8),
    national Nullable(UInt8),
    duplicateof_id Nullable(Int32),
    nationalteamplayerscount Nullable(Int32),
    foreignplayerscount Nullable(Int32),
    parentteam_id Nullable(Int32),
    managercontractuntil Nullable(DateTime64(6)),
    country_alpha2 Nullable(String),
    type         Int32,
    playerteaminfo_id Nullable(Int32),
    primaryuniquetournament_id Nullable(Int32),
    thirdcolor_hstripes Nullable(Int32),
    thirdcolor_main Nullable(Int32),
    thirdcolor_number Nullable(Int32),
    thirdcolor_sleeve Nullable(Int32),
    thirdcolor_split Nullable(Int32),
    thirdcolor_squares Nullable(Int32),
    thirdcolor_vstripes Nullable(Int32),
    thirdcolor_outline Nullable(Int32),
    squadprovider Nullable(Int16)
)

PRIMARY KEY id
SOURCE (CLICKHOUSE(
        DATABASE 'sports'
        TABLE 'team'
        USER 'mharalovic'
        PASSWORD 'Fs75EePJ3m54EyysB75U'
))
LAYOUT (HASHED()) -- chosen due to 127446 unique ids
LIFETIME(MIN 0 MAX 1000);

CREATE DICTIONARY  mharalovic.player_dictionary
(
    id                Int32,
    team_id           Int32,
    name              String,
    position Nullable(String),
    weight Nullable(Int32),
    height Nullable(Int32),
    preferredfoot Nullable(String),
    externalid Nullable(Int32),
    externaltype      Int16,
    image_id Nullable(Int32),
    marketvalue Nullable(Int32),
    marketvaluediff Nullable(Float64),
    hastransferhistory Nullable(UInt8),
    contractuntil Nullable(DateTime64(6)),
    retired Nullable(UInt8),
    duplicateof_id Nullable(Int32),
    shortname Nullable(String),
    usercount         Int32,
    extra Nullable(String),
    slug              String,
    disabled Nullable(UInt8),
    lockedposition Nullable(UInt8),
    deceased Nullable(UInt8),
    country_alpha2 Nullable(String),
    managerrole_id Nullable(Int32),
    firstname Nullable(String),
    lastname Nullable(String),
    positionsdetailed Array(Nullable(String)),
    gender Nullable(String),
    jerseynumber Nullable(String),
    dateofdeath Nullable(Date),
    hasheatmapdata    UInt8,
    dob Nullable(Date),
    injury_id Nullable(Int32)
)
PRIMARY KEY id
SOURCE (CLICKHOUSE(
        DATABASE 'sports'
        TABLE 'team'
        USER 'mharalovic'
        PASSWORD 'Fs75EePJ3m54EyysB75U'
))
LAYOUT (HASHED()) -- chosen due to 296247 unique  ids
LIFETIME(MIN 0 MAX 1000);