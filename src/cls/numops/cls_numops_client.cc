#include "objclass/objclass.h"
#include "cls/numops/cls_numops_client.h"
#include "include/encoding.h"

#include <cstdlib>
#include <errno.h>
#include <sstream>

namespace rados {
  namespace cls {
    namespace numops {

      int add(librados::IoCtx *ioctx,
              const std::string& oid,
              const std::string& key,
              double value_to_add)
      {
        bufferlist in, out;
        ::encode(key, in);

        std::stringstream stream;
        stream << value_to_add;

        ::encode(stream.str(), in);

        return ioctx->exec(oid, "numops", "add", in, out);
      }

      int sub(librados::IoCtx *ioctx,
              const std::string& oid,
              const std::string& key,
              double value_to_subtract)
      {
        return add(ioctx, oid, key, -value_to_subtract);
      }

      int mul(librados::IoCtx *ioctx,
              const std::string& oid,
              const std::string& key,
              double value_to_multiply)
      {
        bufferlist in, out;
        ::encode(key, in);

        std::stringstream stream;
        stream << value_to_multiply;

        ::encode(stream.str(), in);

        return ioctx->exec(oid, "numops", "mul", in, out);
      }

      int div(librados::IoCtx *ioctx,
              const std::string& oid,
              const std::string& key,
              double value_to_divide)
      {
        if (value_to_divide == 0)
          return -EINVAL;

        return mul(ioctx, oid, key, 1 / value_to_divide);
      }

    } // namespace numops
  } // namespace cls
} // namespace rados
