#ifndef NETLINK_H
#define NETLINK_H

#include <glib-2.0/glib.h>

#include "base_types.h"

typedef void (*NetlinkCallback)(gpointer user_data, const char *ifname,
                                gboolean up);

void netlink_add_notify(ColodNetlink *this, NetlinkCallback _func,
                        gpointer user_data);
void netlink_del_notify(ColodNetlink *this, NetlinkCallback _func,
                        gpointer user_data);

int netlink_request_status(ColodNetlink *this, GError **errp);
void netlink_free(ColodNetlink *this);
ColodNetlink *netlink_new(GError **errp);

#endif // NETLINK_H
