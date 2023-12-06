SELECT partman.create_parent(
p_parent_table => 'doma_idds.contents',
p_control => 'request_id',
p_type => 'native',
p_interval=> '1000',
p_premake => 3
);