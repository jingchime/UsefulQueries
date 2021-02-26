select item_id as user_id, created_at, *
from analytics.looker.versions_pivot 
where item_type='User' and object in ('email','phone');
