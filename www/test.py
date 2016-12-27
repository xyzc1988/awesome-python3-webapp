import orm,asyncio

from Models import User,Blog,Comment

async def destroy_pool():
    if orm.__pool is not None:
        orm.__pool.close()
        await orm.__pool.wait_closed()

async def test():
    try:
        await orm.create_pool(loop=loop,user='root',password='root',db='awesome')
        await u.save()
    except Exception as e:
        raise e
    finally:
        await destroy_pool()
    
if __name__ == '__main__':
     u = User(name='Test', email='test@example.com', passwd='1234567890', image='about:blank')
     loop = asyncio.get_event_loop()
     loop.run_until_complete(test())
     loop.close()