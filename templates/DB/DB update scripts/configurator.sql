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