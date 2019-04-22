from google.cloud import pubsub_v1
import argparse


def list_topics(project):

    client = pubsub_v1.PublisherClient()

    project = client.project_path(project)

    for topics_name in client.list_topics(project):
        print(topics_name)

def create_topics(project,topic_name):
    client = pubsub_v1.PublisherClient()
    name = client.topic_path(project,topic_name)
    response = client.create_topic(name)
    return response


def delete_topic(project,topic_name):

    client = pubsub_v1.PublisherClient()

    delet = client.topic_path(project,topic_name)
    response= client.delete_topic(delet)
    return response


def get_topic(project,topic_name):

    client = pubsub_v1.PublisherClient()

    name = client.topic_path(project,topic_name)

    response = client.get_topic(name)

    return response



def list_subscription(project,topic_name):

    client = pubsub_v1.PublisherClient()

    name = client.topic_path(project,topic_name)

    for element in client.list_topic_subscriptions(name):
        print(element)

#print(list_subscription(project,'my_topics_test'))

#print(list_topics(project))

def publish_message(project,topic_name,data):

    client = pubsub_v1.PublisherClient()
    topic  = client.topic_path(project,topic_name)
    data = data.encode('utf-8')
    resonse = client.publish(topic,data)
    return resonse

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)

    sub_parser = parser.add_subparsers(dest='command')
    list_parser = sub_parser.add_parser('list',help=list_topics.__doc__)
    list_parser.add_argument('project')

    create_parser = sub_parser.add_parser('create',help=create_topics.__doc__)
    create_parser.add_argument('project')
    create_parser.add_argument('topic_name')

    delete_parser = sub_parser.add_parser('delete', help=delete_topic.__doc__)
    delete_parser.add_argument('project')
    delete_parser.add_argument('topic_name')

    get_parser = sub_parser.add_parser('get', help=get_topic.__doc__)
    get_parser.add_argument('project')
    get_parser.add_argument('topic_name')

    publish_parser = sub_parser.add_parser('publish', help=publish_message.__doc__)
    publish_parser.add_argument('project')
    publish_parser.add_argument('topic_name')
    publish_parser.add_argument('data')

    subcribe_parser = sub_parser.add_parser('list_subc', help=list_subscription.__doc__)
    subcribe_parser.add_argument('project')
    subcribe_parser.add_argument('topic_name')

    args = parser.parse_args()

    if args.command == 'list':
        list_topics(args.project)
    elif args.command == 'create':
        create_topics(args.project,args.topic_name)
    elif args.command == 'delete':
        delete_topic(args.project,args.topic_name)
    elif args.command == 'get':
        get_topic(args.project,args.topic_name)
    elif args.command == 'publish':
        publish_message(args.project, args.topic_name,args.data)
    elif args.command == 'list_subc':
        list_subscription(args.project,args.topic_name)



