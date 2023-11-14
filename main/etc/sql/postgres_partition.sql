SELECT partman.create_parent(
p_parent_table => 'doma_idds.contents',
p_control => 'content_id',
p_type => 'native',
p_interval=> '1000000',
p_premake => 3
);