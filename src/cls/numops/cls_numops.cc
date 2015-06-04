/** \file
 *
 * This is an OSD class that implements methods for object numeric options on
 * its omap values.
 *
 */

#include "objclass/objclass.h"
#include <asm-generic/errno.h>
#include <iostream>
#include <map>
#include <string>
#include <sstream>
#include <cstdio>

CLS_VER(1,0)
CLS_NAME(numops)

cls_handle_t h_class;
cls_method_handle_t h_add;
cls_method_handle_t h_mul;

static int add(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string key, diff_str;

  bufferlist::iterator iter = in->begin();
  try {
    ::decode(key, iter);
    ::decode(diff_str, iter);
  } catch (const buffer::error &err) {
    CLS_LOG(20, "add: invalid decode of input");
    return -EINVAL;
  }

  char *end_ptr = 0;
  double difference = strtod(diff_str.c_str(), &end_ptr);

  if (end_ptr && *end_ptr != '\0') {
    CLS_ERR("add: invalid input value: %s", diff_str.c_str());
    return -EINVAL;
  }

  bufferlist bl;
  int ret = cls_cxx_map_get_val(hctx, key, &bl);

  double value;

  if (ret == -ENODATA || bl.length() == 0) {
    value = 0;
  } else if (ret < 0) {
    if (ret != -ENOENT) {
      CLS_ERR("add: error reading omap key %s: %d", key.c_str(), ret);
    }
    return ret;
  } else {
    std::string stored_value(bl.c_str(), bl.length());
    end_ptr = 0;
    value = strtod(stored_value.c_str(), &end_ptr);

    if (end_ptr && *end_ptr != '\0') {
      CLS_ERR("add: invalid stored value: %s", stored_value.c_str());
      return -EBADMSG;
    }
  }

  value += difference;

  std::stringstream stream;

  stream.str("");
  stream << value;

  bufferlist new_value;
  new_value.append(stream.str());

  return cls_cxx_map_set_val(hctx, key, &new_value);
}

static int mul(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string key, diff_str;

  bufferlist::iterator iter = in->begin();
  try {
    ::decode(key, iter);
    ::decode(diff_str, iter);
  } catch (const buffer::error &err) {
    CLS_LOG(20, "add: invalid decode of input");
    return -EINVAL;
  }

  char *end_ptr = 0;
  double difference = strtod(diff_str.c_str(), &end_ptr);

  if (end_ptr && *end_ptr != '\0') {
    CLS_ERR("add: invalid input value: %s", diff_str.c_str());
    return -EINVAL;
  }

  bufferlist bl;
  int ret = cls_cxx_map_get_val(hctx, key, &bl);

  double value;

  if (ret == -ENODATA || bl.length() == 0) {
    value = 0;
  } else if (ret < 0) {
    if (ret != -ENOENT) {
      CLS_ERR("add: error reading omap key %s: %d", key.c_str(), ret);
    }
    return ret;
  } else {
    std::string stored_value(bl.c_str(), bl.length());
    end_ptr = 0;
    value = strtod(stored_value.c_str(), &end_ptr);

    if (end_ptr && *end_ptr != '\0') {
      CLS_ERR("add: invalid stored value: %s", stored_value.c_str());
      return -EBADMSG;
    }
  }

  value *= difference;

  std::stringstream stream;

  stream.str("");
  stream << value;

  bufferlist new_value;
  new_value.append(stream.str());

  return cls_cxx_map_set_val(hctx, key, &new_value);
}

void __cls_init()
{
  CLS_LOG(20, "loading cls_numops");

  cls_register("numops", &h_class);

  cls_register_cxx_method(h_class, "add",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          add, &h_add);

  cls_register_cxx_method(h_class, "mul",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          mul, &h_mul);
}
