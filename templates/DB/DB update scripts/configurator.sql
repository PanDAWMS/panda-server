create table atlas_panda.site
(
site_name varchar(52),
role varchar(256),
constraint site_name_pk primary key(site_name),
state varchar(20)
)

create table atlas_panda.panda_site
(
panda_site_name varchar(52),
site_name varchar(52),
role varchar(256),
constraint panda_site_name_pk primary key(panda_site_name),
constraint site_fk foreign key(site_name) references atlas_panda.site(site_name),
state varchar(20)
)

create table atlas_panda.ddm_endpoint
(
ddm_endpoint_name varchar(52),
site_name varchar(52),
ddm_spacetoken_name varchar(52),
constraint ddm_endpoint_name_pk primary key(ddm_endpoint_name),
constraint ddm_site_fk foreign key(site_name) references atlas_panda.site(site_name),
state varchar(20)
)

--m to n relationship between panda and ddm sites 
create table atlas_panda.panda_ddm_relation
(
panda_site_name varchar(52),
ddm_endpoint_name varchar(52),
is_local char,  --Y/N. Is the DDM site local to the panda site?
is_default char, --Y/N. Is it the default DDM site for the panda site?
constraint panda_ddm_relation_pk primary key(panda_site_name, panda_ddm_name),
constraint panda_site_fk foreign key(panda_site_name) references atlas_panda.panda_site(panda_site_name),
constraint panda_ddm_endpoint_fk foreign key(panda_ddm_name) references atlas_panda.ddm_endpoint(ddm_endpoint_name)
)

select * from ATLAS_PANDA.site s, ATLAS_PANDA.panda_site ps, ATLAS_PANDA.ddm_endpoint de, ATLAS_PANDA.panda_ddm_relation pdr
where s.site_name = ps.site_name
and s.site_name = de.site_name
and pdr.panda_site_name = ps.panda_site_name
and pdr.ddm_endpoint_name = de.ddm_endpoint_name
and s.site_name like '%HELIX%'

select * from ATLAS_PANDA.site s
where s.site_name like '%HELIX%'

select * from ATLAS_PANDA.panda_site
where site_name like '%HELIX%'

select * from ATLAS_PANDA.ddm_endpoint
where site_name like '%HELIX%'

select * from ATLAS_PANDA.panda_ddm_relation
order by 
where panda_site_name like '%HELIX%'

select * from ATLAS_PANDAMETA.schedconfig
where nickname like '%Titan%'


