from google.cloud import pubsub_v1
import argparse


def list_subscription(project,topic_name):

    client = pubsub_v1.PublisherClient()
    topic = client.topic_path(project,topic_name)

    for subs in client.list_topic_subscriptions(topic):
        print(subs)

def create_subscription(project,topic_name,subscrition):

    client = pubsub_v1.SubscriberClient()
    name = client.subscription_path(project,subscrition)
    topic = client.topic_path(project,topic_name)
    response = client.create_subscription(name,topic)
    return response

#print(create_subscription('project','my_topics_test','mysubsciption2'))

#print(list_subscription('project','my_topics_test'))

def delete_subscription(project,subscription):

    client = pubsub_v1.SubscriberClient()
    name = client.subscription_path(project,subscription)
    response = client.delete_subscription(name)
    return response

def recieve_messages(project,subcription):
    subscription_client = pubsub_v1.SubscriberClient()
    name = subscription_client.subscription_path(project,subcription)

    def callback(message):
        print("receive message : {}" .format(message))
        message.ack()
    data_response = subscription_client.subscribe(name,callback=callback)
    try:
        data_response.result(timeout=10)
    except Exception as e:
        print("Listening for the message on {} threw an exception {} ".format(subcription,e))


#print(delete_subscription('project','my_topics_test','mysubsciption1'))
# print(list_subscription('project','my_topics_test'))
# print(recieve_messages("project","mysubsciption1"))

if '__name__' == '__main__':

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)

    sub_parser = parser.add_subparsers(dest='command')
    list_subscription = sub_parser.add_parser('list', help=list_subscription.__doc__)
    list_subscription.add_argument('project')
    list_subscription.add_argument('topic_name')

    create_subscription = sub_parser.add_parser('create', help=create_subscription.__doc__)
    create_subscription.add_argument('project')
    create_subscription.add_argument('topic_name')
    create_subscription.add_argument('subscrition')

    delete_parser = sub_parser.add_parser('delete', help=delete_subscription.__doc__)
    delete_parser.add_argument('project')
    delete_parser.add_argument('subsciption')

    receive_parser = sub_parser.add_parser('receive', help=recieve_messages.__doc__)
    receive_parser.add_argument('project')
    receive_parser.add_argument('subsciption')

    args = parser.parse_args()

    if args.command == 'list':
        list_subscription(args.project)
    elif args.command == 'create':
        create_subscription(args.project, args.topic_name)
    elif args.command == 'delete':
        delete_subscription(args.project, args.topic_name)
    elif args.command == 'receive':
        recieve_messages(args.project, args.topic_name)



